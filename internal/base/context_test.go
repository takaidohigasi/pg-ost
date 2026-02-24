/*
   Copyright 2024
   pg-ost: PostgreSQL Online Schema Transformation
*/

package base

import (
	"sync"
	"testing"
	"time"
)

func TestNewMigrationContext(t *testing.T) {
	ctx := NewMigrationContext()

	if ctx.Uuid == "" {
		t.Error("Uuid should not be empty")
	}

	if ctx.SchemaName != "public" {
		t.Errorf("SchemaName = %q, want 'public'", ctx.SchemaName)
	}

	if ctx.ChunkSize != 1000 {
		t.Errorf("ChunkSize = %d, want 1000", ctx.ChunkSize)
	}

	if ctx.MaxLagMillisecondsThrottleThreshold != 1500 {
		t.Errorf("MaxLagMillisecondsThrottleThreshold = %d, want 1500", ctx.MaxLagMillisecondsThrottleThreshold)
	}
}

func TestGetTableNames(t *testing.T) {
	ctx := NewMigrationContext()
	ctx.OriginalTableName = "users"

	ghost := ctx.GetGhostTableName()
	if ghost != "_users_gho" {
		t.Errorf("GetGhostTableName() = %q, want '_users_gho'", ghost)
	}

	changelog := ctx.GetChangelogTableName()
	if changelog != "_users_ghc" {
		t.Errorf("GetChangelogTableName() = %q, want '_users_ghc'", changelog)
	}

	ctx.TimestampOldTable = false
	old := ctx.GetOldTableName()
	if old != "_users_old" {
		t.Errorf("GetOldTableName() = %q, want '_users_old'", old)
	}
}

func TestAtomicCounters(t *testing.T) {
	ctx := NewMigrationContext()

	// Test TotalRowsCopied
	ctx.AddTotalRowsCopied(100)
	if ctx.GetTotalRowsCopied() != 100 {
		t.Errorf("GetTotalRowsCopied() = %d, want 100", ctx.GetTotalRowsCopied())
	}
	ctx.AddTotalRowsCopied(50)
	if ctx.GetTotalRowsCopied() != 150 {
		t.Errorf("GetTotalRowsCopied() = %d, want 150", ctx.GetTotalRowsCopied())
	}

	// Test TotalDMLEventsApplied
	ctx.AddTotalDMLEventsApplied(25)
	if ctx.GetTotalDMLEventsApplied() != 25 {
		t.Errorf("GetTotalDMLEventsApplied() = %d, want 25", ctx.GetTotalDMLEventsApplied())
	}

	// Test Iteration
	ctx.IncrementIteration()
	ctx.IncrementIteration()
	if ctx.GetIteration() != 2 {
		t.Errorf("GetIteration() = %d, want 2", ctx.GetIteration())
	}
}

func TestThrottleControl(t *testing.T) {
	ctx := NewMigrationContext()

	// Initially not throttled
	if ctx.HasThrottleControlReasonHint() {
		t.Error("Should not have throttle control initially")
	}

	// Set throttle
	ctx.SetThrottleControlReasonHint()
	if !ctx.HasThrottleControlReasonHint() {
		t.Error("Should have throttle control after setting")
	}

	// Clear throttle
	ctx.ClearThrottleControlReasonHint()
	if ctx.HasThrottleControlReasonHint() {
		t.Error("Should not have throttle control after clearing")
	}
}

func TestThrottleState(t *testing.T) {
	ctx := NewMigrationContext()

	// Set throttle
	ctx.SetThrottle(true, "test reason", UserCommandThrottleReasonHint)

	throttled, reason, hint := ctx.IsThrottled()
	if !throttled {
		t.Error("Should be throttled")
	}
	if reason != "test reason" {
		t.Errorf("Reason = %q, want 'test reason'", reason)
	}
	if hint != UserCommandThrottleReasonHint {
		t.Errorf("Hint = %v, want UserCommandThrottleReasonHint", hint)
	}

	// Clear throttle
	ctx.SetThrottle(false, "", NoThrottleReasonHint)
	throttled, _, _ = ctx.IsThrottled()
	if throttled {
		t.Error("Should not be throttled")
	}
}

func TestCutOverFlags(t *testing.T) {
	ctx := NewMigrationContext()

	// Initially not in critical section
	if ctx.IsInCutOverCriticalSection() {
		t.Error("Should not be in critical section initially")
	}

	// Set critical section
	ctx.SetInCutOverCriticalSection(true)
	if !ctx.IsInCutOverCriticalSection() {
		t.Error("Should be in critical section")
	}

	// Clear critical section
	ctx.SetInCutOverCriticalSection(false)
	if ctx.IsInCutOverCriticalSection() {
		t.Error("Should not be in critical section")
	}

	// Mark cutover complete
	ctx.MarkCutOverComplete()
	// This sets CutOverCompleteFlag to 1
}

func TestProgress(t *testing.T) {
	ctx := NewMigrationContext()
	ctx.RowsEstimate = 1000
	ctx.AddTotalRowsCopied(500)

	progress := ctx.GetProgressPct()
	if progress != 50.0 {
		t.Errorf("GetProgressPct() = %f, want 50.0", progress)
	}

	// Test with zero estimate
	ctx2 := NewMigrationContext()
	ctx2.RowsEstimate = 0
	if ctx2.GetProgressPct() != 0 {
		t.Errorf("GetProgressPct() with zero estimate = %f, want 0", ctx2.GetProgressPct())
	}
}

func TestHeartbeat(t *testing.T) {
	ctx := NewMigrationContext()

	// Initially no heartbeat
	duration := ctx.TimeSinceLastHeartbeatOnChangelog()
	if duration != 0 {
		t.Errorf("TimeSinceLastHeartbeatOnChangelog() = %v, want 0", duration)
	}

	// Set heartbeat
	ctx.SetLastHeartbeatOnChangelogTime(time.Now())
	time.Sleep(10 * time.Millisecond)

	duration = ctx.TimeSinceLastHeartbeatOnChangelog()
	if duration < 10*time.Millisecond {
		t.Errorf("TimeSinceLastHeartbeatOnChangelog() = %v, expected >= 10ms", duration)
	}
}

func TestRowCopyTime(t *testing.T) {
	ctx := NewMigrationContext()

	// Initially zero
	if ctx.ElapsedRowCopyTime() != 0 {
		t.Errorf("ElapsedRowCopyTime() should be 0 initially")
	}

	// Start row copy
	ctx.MarkRowCopyStartTime()
	time.Sleep(50 * time.Millisecond)

	elapsed := ctx.ElapsedRowCopyTime()
	if elapsed < 50*time.Millisecond {
		t.Errorf("ElapsedRowCopyTime() = %v, expected >= 50ms", elapsed)
	}

	// End row copy
	ctx.MarkRowCopyEndTime()
	finalElapsed := ctx.ElapsedRowCopyTime()

	// Should be roughly the same (not increasing anymore)
	time.Sleep(10 * time.Millisecond)
	if ctx.ElapsedRowCopyTime() != finalElapsed {
		t.Error("ElapsedRowCopyTime() should not change after MarkRowCopyEndTime()")
	}
}

func TestConcurrentAccess(t *testing.T) {
	ctx := NewMigrationContext()

	var wg sync.WaitGroup
	numGoroutines := 100
	iterations := 100

	// Test concurrent counter updates
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				ctx.AddTotalRowsCopied(1)
				ctx.AddTotalDMLEventsApplied(1)
				ctx.IncrementIteration()
			}
		}()
	}

	wg.Wait()

	expected := int64(numGoroutines * iterations)
	if ctx.GetTotalRowsCopied() != expected {
		t.Errorf("GetTotalRowsCopied() = %d, want %d", ctx.GetTotalRowsCopied(), expected)
	}
	if ctx.GetTotalDMLEventsApplied() != expected {
		t.Errorf("GetTotalDMLEventsApplied() = %d, want %d", ctx.GetTotalDMLEventsApplied(), expected)
	}
	if ctx.GetIteration() != expected {
		t.Errorf("GetIteration() = %d, want %d", ctx.GetIteration(), expected)
	}
}

func TestReplicaConnection(t *testing.T) {
	ctx := NewMigrationContext()

	// Initially no replica configured
	if ctx.HasReplicaConnection() {
		t.Error("Should not have replica connection initially")
	}

	// Set replica host
	ctx.ReplicaHost = "replica.example.com"
	ctx.ReplicaPort = 5433

	if !ctx.HasReplicaConnection() {
		t.Error("Should have replica connection after setting ReplicaHost")
	}

	hostPort := ctx.GetReplicaHostPort()
	if hostPort != "replica.example.com:5433" {
		t.Errorf("GetReplicaHostPort() = %q, want 'replica.example.com:5433'", hostPort)
	}
}
