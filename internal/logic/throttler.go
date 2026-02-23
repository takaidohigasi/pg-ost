/*
   Copyright 2024
   pg-ost: PostgreSQL Online Schema Transformation
*/

package logic

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/your-org/pg-ost/internal/base"
)

// Throttler controls migration speed based on various conditions
type Throttler struct {
	migrationContext *base.MigrationContext
	applier          *Applier
	inspector        *Inspector
	httpClient       *http.Client

	lastThrottleCheck time.Time
	lastHTTPCheck     time.Time
}

// NewThrottler creates a new Throttler
func NewThrottler(ctx *base.MigrationContext, applier *Applier, inspector *Inspector) *Throttler {
	return &Throttler{
		migrationContext: ctx,
		applier:          applier,
		inspector:        inspector,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

// Run starts the throttle checking loop
func (t *Throttler) Run() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	heartbeatTicker := time.NewTicker(time.Duration(t.migrationContext.HeartbeatIntervalMilliseconds) * time.Millisecond)
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-ticker.C:
			t.checkThrottle()
		case <-heartbeatTicker.C:
			t.writeHeartbeat()
		}

		// Check if we should stop
		if t.migrationContext.CanStopStreaming() {
			return
		}
	}
}

// checkThrottle performs all throttle checks
func (t *Throttler) checkThrottle() {
	throttle, reason, hint := t.shouldThrottle()
	t.migrationContext.SetThrottle(throttle, reason, hint)

	if throttle {
		t.migrationContext.Log.Debug("Throttled: %s", reason)
	}
}

// shouldThrottle determines if migration should be throttled
func (t *Throttler) shouldThrottle() (bool, string, base.ThrottleReasonHint) {
	// Check if in critical section (cutover)
	if t.migrationContext.IsInCutOverCriticalSection() {
		return false, "", base.NoThrottleReasonHint
	}

	// Check user-commanded throttle
	if t.migrationContext.HasThrottleControlReasonHint() {
		return true, "user-commanded throttle", base.UserCommandThrottleReasonHint
	}

	// Check hibernation
	hibernateUntil := atomic.LoadInt64(&t.migrationContext.HibernateUntil)
	if hibernateUntil > 0 && time.Now().UnixNano() < hibernateUntil {
		return true, "hibernating", base.LeavingHibernationThrottleReasonHint
	}

	// Check throttle flag files
	if t.migrationContext.ThrottleFlagFile != "" {
		if _, err := os.Stat(t.migrationContext.ThrottleFlagFile); err == nil {
			return true, fmt.Sprintf("throttle flag file exists: %s", t.migrationContext.ThrottleFlagFile), base.NoThrottleReasonHint
		}
	}
	if t.migrationContext.ThrottleAdditionalFlagFile != "" {
		if _, err := os.Stat(t.migrationContext.ThrottleAdditionalFlagFile); err == nil {
			return true, fmt.Sprintf("throttle flag file exists: %s", t.migrationContext.ThrottleAdditionalFlagFile), base.NoThrottleReasonHint
		}
	}

	// Check HTTP throttle
	if t.migrationContext.ThrottleHTTP != "" {
		if throttle, reason := t.checkHTTPThrottle(); throttle {
			return true, reason, base.NoThrottleReasonHint
		}
	}

	// Check throttle query
	if t.migrationContext.ThrottleQuery != "" {
		if throttle, reason := t.checkThrottleQuery(); throttle {
			return true, reason, base.NoThrottleReasonHint
		}
	}

	// Check replication lag
	if throttle, reason := t.checkReplicationLag(); throttle {
		return true, reason, base.NoThrottleReasonHint
	}

	// Check max load
	if len(t.migrationContext.MaxLoad) > 0 {
		if throttle, reason := t.checkMaxLoad(); throttle {
			return true, reason, base.NoThrottleReasonHint
		}
	}

	// Check critical load
	if len(t.migrationContext.CriticalLoad) > 0 {
		if hibernate, reason := t.checkCriticalLoad(); hibernate {
			// Set hibernation
			duration := time.Duration(t.migrationContext.CriticalLoadHibernateSeconds) * time.Second
			atomic.StoreInt64(&t.migrationContext.HibernateUntil, time.Now().Add(duration).UnixNano())
			return true, reason, base.NoThrottleReasonHint
		}
	}

	return false, "", base.NoThrottleReasonHint
}

// checkHTTPThrottle checks the HTTP throttle endpoint
func (t *Throttler) checkHTTPThrottle() (bool, string) {
	// Rate limit HTTP checks
	if time.Since(t.lastHTTPCheck) < time.Duration(t.migrationContext.ThrottleHTTPIntervalMillis)*time.Millisecond {
		return false, ""
	}
	t.lastHTTPCheck = time.Now()

	resp, err := t.httpClient.Head(t.migrationContext.ThrottleHTTP)
	if err != nil {
		if t.migrationContext.IgnoreHTTPErrors {
			return false, ""
		}
		return true, fmt.Sprintf("HTTP throttle error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != base.HTTPStatusOK {
		return true, fmt.Sprintf("HTTP throttle: status %d", resp.StatusCode)
	}

	return false, ""
}

// checkThrottleQuery executes the throttle query
func (t *Throttler) checkThrottleQuery() (bool, string) {
	if t.inspector == nil || t.inspector.db == nil {
		return false, ""
	}

	ctx := context.Background()
	var result int
	err := t.inspector.db.QueryRowContext(ctx, t.migrationContext.ThrottleQuery).Scan(&result)
	if err != nil {
		if err != sql.ErrNoRows {
			return true, fmt.Sprintf("throttle query error: %v", err)
		}
		return false, ""
	}

	if result > 0 {
		return true, fmt.Sprintf("throttle query returned %d", result)
	}

	return false, ""
}

// checkReplicationLag checks if replication lag exceeds threshold
func (t *Throttler) checkReplicationLag() (bool, string) {
	// Check heartbeat-based lag
	lag := t.migrationContext.TimeSinceLastHeartbeatOnChangelog()
	if lag > 0 {
		lagMs := lag.Milliseconds()
		atomic.StoreInt64(&t.migrationContext.CurrentLag, int64(lag))

		if lagMs > t.migrationContext.MaxLagMillisecondsThrottleThreshold {
			return true, fmt.Sprintf("replication lag: %dms > %dms",
				lagMs, t.migrationContext.MaxLagMillisecondsThrottleThreshold)
		}
	}

	return false, ""
}

// checkMaxLoad checks if current load exceeds max-load thresholds
func (t *Throttler) checkMaxLoad() (bool, string) {
	if t.inspector == nil || t.inspector.db == nil {
		return false, ""
	}

	ctx := context.Background()

	for metric, threshold := range t.migrationContext.MaxLoad {
		value, err := t.getLoadMetric(ctx, metric)
		if err != nil {
			continue
		}

		if value >= threshold {
			return true, fmt.Sprintf("max-load exceeded: %s=%d >= %d", metric, value, threshold)
		}
	}

	return false, ""
}

// checkCriticalLoad checks if current load exceeds critical-load thresholds
func (t *Throttler) checkCriticalLoad() (bool, string) {
	if t.inspector == nil || t.inspector.db == nil {
		return false, ""
	}

	ctx := context.Background()

	for metric, threshold := range t.migrationContext.CriticalLoad {
		value, err := t.getLoadMetric(ctx, metric)
		if err != nil {
			continue
		}

		if value >= threshold {
			return true, fmt.Sprintf("critical-load exceeded: %s=%d >= %d (hibernating)", metric, value, threshold)
		}
	}

	return false, ""
}

// getLoadMetric retrieves a load metric value
func (t *Throttler) getLoadMetric(ctx context.Context, metric string) (int64, error) {
	var value int64
	var query string

	switch metric {
	case "active_connections", "numbackends":
		query = "SELECT numbackends FROM pg_stat_database WHERE datname = current_database()"
	case "active_queries":
		query = `SELECT COUNT(*) FROM pg_stat_activity
				WHERE state = 'active' AND pid != pg_backend_pid()`
	case "idle_in_transaction":
		query = `SELECT COUNT(*) FROM pg_stat_activity
				WHERE state = 'idle in transaction' AND pid != pg_backend_pid()`
	case "waiting":
		query = `SELECT COUNT(*) FROM pg_stat_activity
				WHERE wait_event_type IS NOT NULL AND pid != pg_backend_pid()`
	case "locks":
		query = "SELECT COUNT(*) FROM pg_locks WHERE NOT granted"
	default:
		// Try as a custom query
		query = fmt.Sprintf("SELECT %s", metric)
	}

	err := t.inspector.db.QueryRowContext(ctx, query).Scan(&value)
	if err != nil {
		return 0, err
	}

	return value, nil
}

// writeHeartbeat writes a heartbeat to the changelog table
func (t *Throttler) writeHeartbeat() {
	if t.applier == nil {
		return
	}

	if err := t.applier.WriteHeartbeat(); err != nil {
		t.migrationContext.Log.Debug("Failed to write heartbeat: %v", err)
	}
}

// UpdateETA calculates and updates the ETA based on current progress
func (t *Throttler) UpdateETA() {
	elapsed := t.migrationContext.ElapsedRowCopyTime()
	if elapsed <= 0 {
		return
	}

	rowsCopied := t.migrationContext.GetTotalRowsCopied()
	if rowsCopied <= 0 {
		return
	}

	rowsPerSecond := float64(rowsCopied) / elapsed.Seconds()
	atomic.StoreInt64(&t.migrationContext.EtaRowsPerSecond, int64(rowsPerSecond))
}
