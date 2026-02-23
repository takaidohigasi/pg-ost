/*
   Copyright 2024
   pg-ost: PostgreSQL Online Schema Transformation
*/

package base

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
)

// RowsEstimateMethod indicates how row count was estimated
type RowsEstimateMethod string

const (
	TableStatsRowsEstimate RowsEstimateMethod = "TableStatsRowsEstimate"
	ExplainRowsEstimate    RowsEstimateMethod = "ExplainRowsEstimate"
	CountRowsEstimate      RowsEstimateMethod = "CountRowsEstimate"
)

// ThrottleReasonHint provides context for throttling decisions
type ThrottleReasonHint string

const (
	NoThrottleReasonHint                 ThrottleReasonHint = "NoThrottleReasonHint"
	UserCommandThrottleReasonHint        ThrottleReasonHint = "UserCommandThrottleReasonHint"
	LeavingHibernationThrottleReasonHint ThrottleReasonHint = "LeavingHibernationThrottleReasonHint"
)

const (
	HTTPStatusOK       = 200
	MaxEventsBatchSize = 1000
	ETAUnknown         = math.MinInt64
)

// ThrottleCheckResult contains the result of a throttle check
type ThrottleCheckResult struct {
	ShouldThrottle bool
	Reason         string
	ReasonHint     ThrottleReasonHint
}

// NewThrottleCheckResult creates a new ThrottleCheckResult
func NewThrottleCheckResult(throttle bool, reason string, reasonHint ThrottleReasonHint) *ThrottleCheckResult {
	return &ThrottleCheckResult{
		ShouldThrottle: throttle,
		Reason:         reason,
		ReasonHint:     reasonHint,
	}
}

// MigrationContext holds the global state for a migration.
// It is used by all components throughout the migration process.
type MigrationContext struct {
	Uuid string

	// Database identifiers
	DatabaseName      string
	SchemaName        string // PostgreSQL schema (default: "public")
	OriginalTableName string
	AlterStatement    string

	// Connection settings
	Host     string
	Port     int
	User     string
	Password string
	SSLMode  string

	// Replication settings
	PublicationName     string
	ReplicationSlotName string
	StartLSN            pglogrepl.LSN
	CurrentLSN          pglogrepl.LSN
	lsnMutex            *sync.Mutex

	// Table information
	UniqueKeyColumns       []string
	OriginalTableColumns   []string
	GhostTableName         string
	ChangelogTableName     string
	OldTableName           string
	OriginalReplicaIdentity string

	// Row copy tracking
	MigrationRangeMinValues []interface{}
	MigrationRangeMaxValues []interface{}
	IterationRangeMinValues []interface{}
	IterationRangeMaxValues []interface{}

	// Progress tracking (atomic)
	TotalRowsCopied       int64
	RowsEstimate          int64
	RowsDeltaEstimate     int64
	TotalDMLEventsApplied int64
	Iteration             int64
	CurrentLag            int64 // nanoseconds
	EtaRowsPerSecond      int64

	// Estimation method
	UsedRowsEstimateMethod RowsEstimateMethod

	// Throttle settings
	ChunkSize                           int64
	DMLBatchSize                        int64
	DefaultNumRetries                   int64
	MaxLagMillisecondsThrottleThreshold int64
	HeartbeatIntervalMilliseconds       int64
	NiceRatio                           float64
	ThrottleFlagFile                    string
	ThrottleAdditionalFlagFile          string
	ThrottleQuery                       string
	ThrottleHTTP                        string
	ThrottleHTTPIntervalMillis          int64
	IgnoreHTTPErrors                    bool
	MaxLoad                             LoadMap
	CriticalLoad                        LoadMap
	CriticalLoadIntervalMilliseconds    int64
	CriticalLoadHibernateSeconds        int64
	PostponeCutOverFlagFile             string
	CutOverLockTimeoutSeconds           int64
	PanicFlagFile                       string

	// Control flags (atomic)
	ThrottleCommandedByUser              int64
	HibernateUntil                       int64
	CutOverCompleteFlag                  int64
	InCutOverCriticalSectionFlag         int64
	AllEventsUpToLockProcessedFlag       int64
	CleanupImminentFlag                  int64

	// Server settings
	DropServeSocket bool
	ServeSocketFile string
	ServeTCPPort    int64

	// Operational flags
	Execute                  bool
	Noop                     bool
	OkToDropTable            bool
	InitiallyDropOldTable    bool
	InitiallyDropGhostTable  bool
	TimestampOldTable        bool
	CountTableRows           bool
	ConcurrentCountTableRows bool

	// Hooks
	HooksPath              string
	HooksHintMessage       string
	HooksHintOwner         string
	HooksHintToken         string
	HooksStatusIntervalSec int64

	// Timestamps
	StartTime           time.Time
	RowCopyStartTime    time.Time
	RowCopyEndTime      time.Time
	LockTablesStartTime time.Time
	RenameTablesEndTime time.Time

	// Internal state
	pointOfInterestTime          time.Time
	pointOfInterestTimeMutex     *sync.Mutex
	lastHeartbeatOnChangelogTime time.Time
	lastHeartbeatOnChangelogMutex *sync.Mutex
	throttleMutex                *sync.Mutex
	throttleHTTPMutex            *sync.Mutex
	configMutex                  *sync.Mutex

	// Throttle state
	isThrottled        bool
	throttleReason     string
	throttleReasonHint ThrottleReasonHint
	throttleGeneralCheckResult ThrottleCheckResult

	// Panic channel
	PanicAbort chan error

	// Log interface
	Log Logger
}

// Logger interface for logging
type Logger interface {
	Debug(format string, args ...interface{})
	Info(format string, args ...interface{})
	Warning(format string, args ...interface{})
	Error(format string, args ...interface{})
	Fatal(format string, args ...interface{})
}

// NewMigrationContext creates a new MigrationContext with default values
func NewMigrationContext() *MigrationContext {
	return &MigrationContext{
		Uuid:       uuid.New().String(),
		SchemaName: "public",

		ChunkSize:                           1000,
		DMLBatchSize:                        100,
		DefaultNumRetries:                   60,
		MaxLagMillisecondsThrottleThreshold: 1500,
		HeartbeatIntervalMilliseconds:       500,
		NiceRatio:                           0,
		CutOverLockTimeoutSeconds:           10,
		ThrottleHTTPIntervalMillis:          100,
		HooksStatusIntervalSec:              60,

		MaxLoad:      NewLoadMap(),
		CriticalLoad: NewLoadMap(),

		lsnMutex:                      &sync.Mutex{},
		pointOfInterestTimeMutex:      &sync.Mutex{},
		lastHeartbeatOnChangelogMutex: &sync.Mutex{},
		throttleMutex:                 &sync.Mutex{},
		throttleHTTPMutex:             &sync.Mutex{},
		configMutex:                   &sync.Mutex{},

		PanicAbort: make(chan error, 1),

		Log: &defaultLogger{},
	}
}

// GetGhostTableName returns the ghost table name
func (ctx *MigrationContext) GetGhostTableName() string {
	if ctx.GhostTableName != "" {
		return ctx.GhostTableName
	}
	ctx.GhostTableName = "_" + ctx.OriginalTableName + "_gho"
	return ctx.GhostTableName
}

// GetChangelogTableName returns the changelog table name
func (ctx *MigrationContext) GetChangelogTableName() string {
	if ctx.ChangelogTableName != "" {
		return ctx.ChangelogTableName
	}
	ctx.ChangelogTableName = "_" + ctx.OriginalTableName + "_ghc"
	return ctx.ChangelogTableName
}

// GetOldTableName returns the old table name (for post-cutover)
func (ctx *MigrationContext) GetOldTableName() string {
	if ctx.OldTableName != "" {
		return ctx.OldTableName
	}
	if ctx.TimestampOldTable {
		ctx.OldTableName = "_" + ctx.OriginalTableName + "_" + time.Now().Format("20060102150405") + "_old"
	} else {
		ctx.OldTableName = "_" + ctx.OriginalTableName + "_old"
	}
	return ctx.OldTableName
}

// MaxRetries returns the max number of retries
func (ctx *MigrationContext) MaxRetries() int64 {
	return ctx.DefaultNumRetries
}

// SetCurrentLSN safely sets the current LSN
func (ctx *MigrationContext) SetCurrentLSN(lsn pglogrepl.LSN) {
	ctx.lsnMutex.Lock()
	defer ctx.lsnMutex.Unlock()
	ctx.CurrentLSN = lsn
}

// GetCurrentLSN safely gets the current LSN
func (ctx *MigrationContext) GetCurrentLSN() pglogrepl.LSN {
	ctx.lsnMutex.Lock()
	defer ctx.lsnMutex.Unlock()
	return ctx.CurrentLSN
}

// SetThrottle sets the throttle state
func (ctx *MigrationContext) SetThrottle(throttle bool, reason string, reasonHint ThrottleReasonHint) {
	ctx.throttleMutex.Lock()
	defer ctx.throttleMutex.Unlock()
	ctx.isThrottled = throttle
	ctx.throttleReason = reason
	ctx.throttleReasonHint = reasonHint
}

// IsThrottled returns whether migration is throttled
func (ctx *MigrationContext) IsThrottled() (bool, string, ThrottleReasonHint) {
	ctx.throttleMutex.Lock()
	defer ctx.throttleMutex.Unlock()
	return ctx.isThrottled, ctx.throttleReason, ctx.throttleReasonHint
}

// SetPointOfInterestTime sets the point of interest time
func (ctx *MigrationContext) SetPointOfInterestTime(t time.Time) {
	ctx.pointOfInterestTimeMutex.Lock()
	defer ctx.pointOfInterestTimeMutex.Unlock()
	ctx.pointOfInterestTime = t
}

// GetPointOfInterestTime returns the point of interest time
func (ctx *MigrationContext) GetPointOfInterestTime() time.Time {
	ctx.pointOfInterestTimeMutex.Lock()
	defer ctx.pointOfInterestTimeMutex.Unlock()
	return ctx.pointOfInterestTime
}

// SetLastHeartbeatOnChangelogTime sets the last heartbeat time
func (ctx *MigrationContext) SetLastHeartbeatOnChangelogTime(t time.Time) {
	ctx.lastHeartbeatOnChangelogMutex.Lock()
	defer ctx.lastHeartbeatOnChangelogMutex.Unlock()
	ctx.lastHeartbeatOnChangelogTime = t
}

// TimeSinceLastHeartbeatOnChangelog returns time since last heartbeat
func (ctx *MigrationContext) TimeSinceLastHeartbeatOnChangelog() time.Duration {
	ctx.lastHeartbeatOnChangelogMutex.Lock()
	defer ctx.lastHeartbeatOnChangelogMutex.Unlock()
	if ctx.lastHeartbeatOnChangelogTime.IsZero() {
		return 0
	}
	return time.Since(ctx.lastHeartbeatOnChangelogTime)
}

// MarkRowCopyStartTime marks the start of row copy
func (ctx *MigrationContext) MarkRowCopyStartTime() {
	ctx.RowCopyStartTime = time.Now()
}

// MarkRowCopyEndTime marks the end of row copy
func (ctx *MigrationContext) MarkRowCopyEndTime() {
	ctx.RowCopyEndTime = time.Now()
}

// ElapsedRowCopyTime returns the elapsed time for row copy
func (ctx *MigrationContext) ElapsedRowCopyTime() time.Duration {
	if ctx.RowCopyStartTime.IsZero() {
		return 0
	}
	if ctx.RowCopyEndTime.IsZero() {
		return time.Since(ctx.RowCopyStartTime)
	}
	return ctx.RowCopyEndTime.Sub(ctx.RowCopyStartTime)
}

// GetTotalRowsCopied returns the total rows copied (atomic)
func (ctx *MigrationContext) GetTotalRowsCopied() int64 {
	return atomic.LoadInt64(&ctx.TotalRowsCopied)
}

// AddTotalRowsCopied adds to the total rows copied (atomic)
func (ctx *MigrationContext) AddTotalRowsCopied(n int64) {
	atomic.AddInt64(&ctx.TotalRowsCopied, n)
}

// GetTotalDMLEventsApplied returns the total DML events applied (atomic)
func (ctx *MigrationContext) GetTotalDMLEventsApplied() int64 {
	return atomic.LoadInt64(&ctx.TotalDMLEventsApplied)
}

// AddTotalDMLEventsApplied adds to the total DML events applied (atomic)
func (ctx *MigrationContext) AddTotalDMLEventsApplied(n int64) {
	atomic.AddInt64(&ctx.TotalDMLEventsApplied, n)
}

// GetIteration returns the current iteration (atomic)
func (ctx *MigrationContext) GetIteration() int64 {
	return atomic.LoadInt64(&ctx.Iteration)
}

// IncrementIteration increments the iteration counter (atomic)
func (ctx *MigrationContext) IncrementIteration() int64 {
	return atomic.AddInt64(&ctx.Iteration, 1)
}

// HasThrottleControlReasonHint returns whether there's a user-commanded throttle
func (ctx *MigrationContext) HasThrottleControlReasonHint() bool {
	return atomic.LoadInt64(&ctx.ThrottleCommandedByUser) > 0
}

// SetThrottleControlReasonHint sets user-commanded throttle
func (ctx *MigrationContext) SetThrottleControlReasonHint() {
	atomic.StoreInt64(&ctx.ThrottleCommandedByUser, 1)
}

// ClearThrottleControlReasonHint clears user-commanded throttle
func (ctx *MigrationContext) ClearThrottleControlReasonHint() {
	atomic.StoreInt64(&ctx.ThrottleCommandedByUser, 0)
}

// IsPostponingCutOver returns whether cutover is postponed
func (ctx *MigrationContext) IsPostponingCutOver() bool {
	return atomic.LoadInt64(&ctx.CutOverCompleteFlag) == 0
}

// IsInCutOverCriticalSection returns whether we're in the critical section
func (ctx *MigrationContext) IsInCutOverCriticalSection() bool {
	return atomic.LoadInt64(&ctx.InCutOverCriticalSectionFlag) > 0
}

// SetInCutOverCriticalSection sets the critical section flag
func (ctx *MigrationContext) SetInCutOverCriticalSection(v bool) {
	if v {
		atomic.StoreInt64(&ctx.InCutOverCriticalSectionFlag, 1)
	} else {
		atomic.StoreInt64(&ctx.InCutOverCriticalSectionFlag, 0)
	}
}

// MarkCutOverComplete marks cutover as complete
func (ctx *MigrationContext) MarkCutOverComplete() {
	atomic.StoreInt64(&ctx.CutOverCompleteFlag, 1)
}

// CanStopStreaming returns whether streaming can stop
func (ctx *MigrationContext) CanStopStreaming() bool {
	return atomic.LoadInt64(&ctx.CleanupImminentFlag) > 0
}

// MarkCleanupImminent marks cleanup as imminent
func (ctx *MigrationContext) MarkCleanupImminent() {
	atomic.StoreInt64(&ctx.CleanupImminentFlag, 1)
}

// GetProgressPct returns the progress percentage
func (ctx *MigrationContext) GetProgressPct() float64 {
	rowsEstimate := atomic.LoadInt64(&ctx.RowsEstimate)
	if rowsEstimate == 0 {
		return 0
	}
	totalRowsCopied := ctx.GetTotalRowsCopied()
	return 100.0 * float64(totalRowsCopied) / float64(rowsEstimate)
}

// GetETA returns the estimated time of completion
func (ctx *MigrationContext) GetETA() time.Duration {
	rowsEstimate := atomic.LoadInt64(&ctx.RowsEstimate)
	totalRowsCopied := ctx.GetTotalRowsCopied()
	rowsDeltaEstimate := atomic.LoadInt64(&ctx.RowsDeltaEstimate)

	remainingRows := rowsEstimate - totalRowsCopied + rowsDeltaEstimate
	if remainingRows <= 0 {
		return 0
	}

	etaRowsPerSecond := atomic.LoadInt64(&ctx.EtaRowsPerSecond)
	if etaRowsPerSecond <= 0 {
		return time.Duration(ETAUnknown)
	}

	return time.Duration(remainingRows/etaRowsPerSecond) * time.Second
}

// defaultLogger is a simple default logger
type defaultLogger struct{}

func (l *defaultLogger) Debug(format string, args ...interface{}) {}
func (l *defaultLogger) Info(format string, args ...interface{})  {}
func (l *defaultLogger) Warning(format string, args ...interface{}) {}
func (l *defaultLogger) Error(format string, args ...interface{}) {}
func (l *defaultLogger) Fatal(format string, args ...interface{}) {}
