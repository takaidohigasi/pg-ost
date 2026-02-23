/*
   Copyright 2024
   pg-ost: PostgreSQL Online Schema Transformation
*/

package config

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config represents the pg-ost configuration file structure
type Config struct {
	Database   DatabaseConfig   `yaml:"database"`
	Replica    *ReplicaConfig   `yaml:"replica,omitempty"`
	Table      TableConfig      `yaml:"table"`
	Processing ProcessingConfig `yaml:"processing"`
	Throttle   ThrottleConfig   `yaml:"throttle"`
	Cutover    CutoverConfig    `yaml:"cutover"`
	Server     *ServerConfig    `yaml:"server,omitempty"`
	Hooks      *HooksConfig     `yaml:"hooks,omitempty"`
}

// DatabaseConfig holds primary database connection settings
type DatabaseConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	SSLMode  string `yaml:"sslmode"`
}

// ReplicaConfig holds replica database connection settings for streaming
type ReplicaConfig struct {
	Host                  string `yaml:"host"`
	Port                  int    `yaml:"port"`
	User                  string `yaml:"user"`
	Password              string `yaml:"password"`
	SSLMode               string `yaml:"sslmode"`
	SkipClusterValidation bool   `yaml:"skip_cluster_validation"`
}

// TableConfig holds table and ALTER statement settings
type TableConfig struct {
	Schema string `yaml:"schema"`
	Name   string `yaml:"name"`
	Alter  string `yaml:"alter"`
}

// ProcessingConfig holds row processing settings
type ProcessingConfig struct {
	ChunkSize           int64   `yaml:"chunk_size"`
	DMLBatchSize        int64   `yaml:"dml_batch_size"`
	NiceRatio           float64 `yaml:"nice_ratio"`
	HeartbeatIntervalMs int64   `yaml:"heartbeat_interval_ms"`
	ExactRowCount       bool    `yaml:"exact_row_count"`
	ConcurrentRowCount  bool    `yaml:"concurrent_row_count"`
}

// ThrottleConfig holds throttling settings
type ThrottleConfig struct {
	MaxLagMs               int64            `yaml:"max_lag_ms"`
	FlagFile               string           `yaml:"flag_file"`
	AdditionalFlagFile     string           `yaml:"additional_flag_file"`
	Query                  string           `yaml:"query"`
	HTTP                   string           `yaml:"http"`
	HTTPIntervalMs         int64            `yaml:"http_interval_ms"`
	IgnoreHTTPErrors       bool             `yaml:"ignore_http_errors"`
	MaxLoad                map[string]int64 `yaml:"max_load"`
	CriticalLoad           map[string]int64 `yaml:"critical_load"`
	CriticalLoadIntervalMs int64            `yaml:"critical_load_interval_ms"`
	CriticalLoadHibernateS int64            `yaml:"critical_load_hibernate_seconds"`
}

// CutoverConfig holds cutover settings
type CutoverConfig struct {
	LockTimeoutSeconds int64  `yaml:"lock_timeout_seconds"`
	PostponeFlagFile   string `yaml:"postpone_flag_file"`
	PanicFlagFile      string `yaml:"panic_flag_file"`
	OkToDropTable      bool   `yaml:"ok_to_drop_table"`
	InitiallyDropOld   bool   `yaml:"initially_drop_old_table"`
	InitiallyDropGhost bool   `yaml:"initially_drop_ghost_table"`
	TimestampOldTable  bool   `yaml:"timestamp_old_table"`
}

// ServerConfig holds online control server settings
type ServerConfig struct {
	SocketFile string `yaml:"socket_file"`
	TCPPort    int64  `yaml:"tcp_port"`
	DropSocket bool   `yaml:"drop_socket"`
}

// HooksConfig holds hook script settings
type HooksConfig struct {
	Path              string `yaml:"path"`
	HintMessage       string `yaml:"hint_message"`
	HintOwner         string `yaml:"hint_owner"`
	HintToken         string `yaml:"hint_token"`
	StatusIntervalSec int64  `yaml:"status_interval_seconds"`
}

// LoadFromFile loads configuration from a YAML file
func LoadFromFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Expand environment variables
	expanded := expandEnvVars(string(data))

	var config Config
	if err := yaml.Unmarshal([]byte(expanded), &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Apply defaults
	applyDefaults(&config)

	// Validate configuration
	if err := validate(&config); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return &config, nil
}

// expandEnvVars expands environment variables in the format ${VAR} or $VAR
func expandEnvVars(content string) string {
	// Match ${VAR} style
	bracketRe := regexp.MustCompile(`\$\{([a-zA-Z_][a-zA-Z0-9_]*)\}`)
	content = bracketRe.ReplaceAllStringFunc(content, func(match string) string {
		varName := match[2 : len(match)-1] // Remove ${ and }
		if val, exists := os.LookupEnv(varName); exists {
			return val
		}
		return match // Keep original if env var not found
	})

	// Match $VAR style (but not inside ${})
	simpleRe := regexp.MustCompile(`\$([a-zA-Z_][a-zA-Z0-9_]*)`)
	content = simpleRe.ReplaceAllStringFunc(content, func(match string) string {
		// Skip if this looks like it was part of ${VAR}
		if strings.HasPrefix(match, "${") {
			return match
		}
		varName := match[1:] // Remove $
		if val, exists := os.LookupEnv(varName); exists {
			return val
		}
		return match // Keep original if env var not found
	})

	return content
}

// applyDefaults sets default values for optional fields
func applyDefaults(cfg *Config) {
	// Database defaults
	if cfg.Database.Host == "" {
		cfg.Database.Host = "localhost"
	}
	if cfg.Database.Port == 0 {
		cfg.Database.Port = 5432
	}
	if cfg.Database.SSLMode == "" {
		cfg.Database.SSLMode = "prefer"
	}

	// Table defaults
	if cfg.Table.Schema == "" {
		cfg.Table.Schema = "public"
	}

	// Processing defaults
	if cfg.Processing.ChunkSize == 0 {
		cfg.Processing.ChunkSize = 1000
	}
	if cfg.Processing.DMLBatchSize == 0 {
		cfg.Processing.DMLBatchSize = 100
	}
	if cfg.Processing.HeartbeatIntervalMs == 0 {
		cfg.Processing.HeartbeatIntervalMs = 500
	}

	// Throttle defaults
	if cfg.Throttle.MaxLagMs == 0 {
		cfg.Throttle.MaxLagMs = 1500
	}
	if cfg.Throttle.HTTPIntervalMs == 0 {
		cfg.Throttle.HTTPIntervalMs = 100
	}

	// Cutover defaults
	if cfg.Cutover.LockTimeoutSeconds == 0 {
		cfg.Cutover.LockTimeoutSeconds = 10
	}

	// Replica defaults (inherit from primary if not specified)
	if cfg.Replica != nil {
		if cfg.Replica.Port == 0 {
			cfg.Replica.Port = 5432
		}
		if cfg.Replica.User == "" {
			cfg.Replica.User = cfg.Database.User
		}
		if cfg.Replica.Password == "" {
			cfg.Replica.Password = cfg.Database.Password
		}
		if cfg.Replica.SSLMode == "" {
			cfg.Replica.SSLMode = cfg.Database.SSLMode
		}
	}

	// Hooks defaults
	if cfg.Hooks != nil && cfg.Hooks.StatusIntervalSec == 0 {
		cfg.Hooks.StatusIntervalSec = 60
	}
}

// ApplyToContext applies the configuration to a MigrationContext
// CLI flags can override these values after this is called
func (cfg *Config) ApplyToContext(ctx interface {
	// We use an interface to avoid circular imports
	// The actual type is *base.MigrationContext
}) {
	// This will be implemented in the CLI where we have access to base package
}

// validate checks that required fields are set and values are valid
func validate(cfg *Config) error {
	// Required database fields
	if cfg.Database.User == "" {
		return fmt.Errorf("database.user is required")
	}
	if cfg.Database.Database == "" {
		return fmt.Errorf("database.database is required")
	}

	// Required table fields
	if cfg.Table.Name == "" {
		return fmt.Errorf("table.name is required")
	}
	if cfg.Table.Alter == "" {
		return fmt.Errorf("table.alter is required")
	}

	// Value constraints
	if cfg.Processing.ChunkSize < 100 || cfg.Processing.ChunkSize > 100000 {
		return fmt.Errorf("processing.chunk_size must be between 100 and 100000")
	}
	if cfg.Processing.NiceRatio < 0 || cfg.Processing.NiceRatio > 10 {
		return fmt.Errorf("processing.nice_ratio must be between 0 and 10")
	}

	// Replica validation
	if cfg.Replica != nil && cfg.Replica.Host == "" {
		return fmt.Errorf("replica.host is required when replica section is specified")
	}

	return nil
}
