/*
   Copyright 2024
   pg-ost: PostgreSQL Online Schema Transformation
*/

package logic

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/your-org/pg-ost/internal/base"
)

// HookType represents the type of hook
type HookType string

const (
	HookOnStartup            HookType = "on-startup"
	HookOnValidated          HookType = "on-validated"
	HookOnRowCountComplete   HookType = "on-row-count-complete"
	HookOnBeforeRowCopy      HookType = "on-before-row-copy"
	HookOnRowCopyComplete    HookType = "on-row-copy-complete"
	HookOnBeforeCutOver      HookType = "on-before-cut-over"
	HookOnInteractive        HookType = "on-interactive"
	HookOnSuccess            HookType = "on-success"
	HookOnFailure            HookType = "on-failure"
	HookOnStatus             HookType = "on-status"
)

// HooksExecutor executes external hook scripts
type HooksExecutor struct {
	migrationContext *base.MigrationContext
}

// NewHooksExecutor creates a new HooksExecutor
func NewHooksExecutor(ctx *base.MigrationContext) *HooksExecutor {
	return &HooksExecutor{
		migrationContext: ctx,
	}
}

// ExecuteHook executes a hook script if it exists
func (e *HooksExecutor) ExecuteHook(hookType HookType) error {
	if e.migrationContext.HooksPath == "" {
		return nil
	}

	hookPath := filepath.Join(e.migrationContext.HooksPath, string(hookType))

	// Check if hook exists
	info, err := os.Stat(hookPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Hook doesn't exist, that's okay
		}
		return fmt.Errorf("failed to stat hook %s: %w", hookPath, err)
	}

	// Check if it's executable
	if info.Mode()&0111 == 0 {
		return fmt.Errorf("hook %s is not executable", hookPath)
	}

	// Execute the hook
	cmd := exec.Command(hookPath)
	cmd.Env = e.buildEnv()
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("hook %s failed: %w", hookType, err)
	}

	e.migrationContext.Log.Debug("Executed hook: %s", hookType)
	return nil
}

// ExecuteHookAsync executes a hook asynchronously
func (e *HooksExecutor) ExecuteHookAsync(hookType HookType) {
	go func() {
		if err := e.ExecuteHook(hookType); err != nil {
			e.migrationContext.Log.Warning("Hook %s failed: %v", hookType, err)
		}
	}()
}

// buildEnv builds the environment variables for hook execution
func (e *HooksExecutor) buildEnv() []string {
	env := os.Environ()

	// Add pg-ost specific environment variables
	ctx := e.migrationContext

	env = append(env,
		fmt.Sprintf("PGOST_DATABASE_NAME=%s", ctx.DatabaseName),
		fmt.Sprintf("PGOST_SCHEMA_NAME=%s", ctx.SchemaName),
		fmt.Sprintf("PGOST_TABLE_NAME=%s", ctx.OriginalTableName),
		fmt.Sprintf("PGOST_GHOST_TABLE_NAME=%s", ctx.GetGhostTableName()),
		fmt.Sprintf("PGOST_OLD_TABLE_NAME=%s", ctx.GetOldTableName()),
		fmt.Sprintf("PGOST_ALTER_STATEMENT=%s", ctx.AlterStatement),
		fmt.Sprintf("PGOST_HOST=%s", ctx.Host),
		fmt.Sprintf("PGOST_PORT=%d", ctx.Port),
		fmt.Sprintf("PGOST_TOTAL_ROWS_COPIED=%d", ctx.GetTotalRowsCopied()),
		fmt.Sprintf("PGOST_TOTAL_DML_EVENTS_APPLIED=%d", ctx.GetTotalDMLEventsApplied()),
		fmt.Sprintf("PGOST_ROWS_ESTIMATE=%d", atomic.LoadInt64(&ctx.RowsEstimate)),
		fmt.Sprintf("PGOST_PROGRESS_PCT=%.2f", ctx.GetProgressPct()),
		fmt.Sprintf("PGOST_ELAPSED_SECONDS=%d", int(time.Since(ctx.StartTime).Seconds())),
		fmt.Sprintf("PGOST_UUID=%s", ctx.Uuid),
	)

	// Add custom hint variables if provided
	if ctx.HooksHintMessage != "" {
		env = append(env, fmt.Sprintf("PGOST_HOOKS_HINT_MESSAGE=%s", ctx.HooksHintMessage))
	}
	if ctx.HooksHintOwner != "" {
		env = append(env, fmt.Sprintf("PGOST_HOOKS_HINT_OWNER=%s", ctx.HooksHintOwner))
	}
	if ctx.HooksHintToken != "" {
		env = append(env, fmt.Sprintf("PGOST_HOOKS_HINT_TOKEN=%s", ctx.HooksHintToken))
	}

	return env
}

// OnStartup executes the on-startup hook
func (e *HooksExecutor) OnStartup() error {
	return e.ExecuteHook(HookOnStartup)
}

// OnValidated executes the on-validated hook
func (e *HooksExecutor) OnValidated() error {
	return e.ExecuteHook(HookOnValidated)
}

// OnRowCountComplete executes the on-row-count-complete hook
func (e *HooksExecutor) OnRowCountComplete() error {
	return e.ExecuteHook(HookOnRowCountComplete)
}

// OnBeforeRowCopy executes the on-before-row-copy hook
func (e *HooksExecutor) OnBeforeRowCopy() error {
	return e.ExecuteHook(HookOnBeforeRowCopy)
}

// OnRowCopyComplete executes the on-row-copy-complete hook
func (e *HooksExecutor) OnRowCopyComplete() error {
	return e.ExecuteHook(HookOnRowCopyComplete)
}

// OnBeforeCutOver executes the on-before-cut-over hook
func (e *HooksExecutor) OnBeforeCutOver() error {
	return e.ExecuteHook(HookOnBeforeCutOver)
}

// OnInteractive executes the on-interactive hook
func (e *HooksExecutor) OnInteractive() error {
	return e.ExecuteHook(HookOnInteractive)
}

// OnSuccess executes the on-success hook
func (e *HooksExecutor) OnSuccess() error {
	return e.ExecuteHook(HookOnSuccess)
}

// OnFailure executes the on-failure hook
func (e *HooksExecutor) OnFailure() error {
	return e.ExecuteHook(HookOnFailure)
}

// OnStatus executes the on-status hook (periodically)
func (e *HooksExecutor) OnStatus() error {
	return e.ExecuteHook(HookOnStatus)
}
