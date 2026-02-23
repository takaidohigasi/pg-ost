/*
   Copyright 2024
   pg-ost: PostgreSQL Online Schema Transformation
*/

package logic

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/takaidohigasi/pg-ost/internal/base"
)

// Server provides runtime control via Unix socket or TCP
type Server struct {
	migrationContext *base.MigrationContext
	hooksExecutor    *HooksExecutor
	printStatus      func() string

	unixListener net.Listener
	tcpListener  net.Listener

	stopChan chan struct{}
}

// NewServer creates a new control server
func NewServer(ctx *base.MigrationContext, hooksExecutor *HooksExecutor, printStatus func() string) *Server {
	return &Server{
		migrationContext: ctx,
		hooksExecutor:    hooksExecutor,
		printStatus:      printStatus,
		stopChan:         make(chan struct{}),
	}
}

// Start starts the control server
func (s *Server) Start() error {
	// Start Unix socket listener
	if s.migrationContext.ServeSocketFile != "" {
		if s.migrationContext.DropServeSocket {
			os.Remove(s.migrationContext.ServeSocketFile)
		}

		listener, err := net.Listen("unix", s.migrationContext.ServeSocketFile)
		if err != nil {
			return fmt.Errorf("failed to create Unix socket: %w", err)
		}
		s.unixListener = listener

		go s.acceptConnections(listener)
		s.migrationContext.Log.Info("Serving on Unix socket: %s", s.migrationContext.ServeSocketFile)
	}

	// Start TCP listener
	if s.migrationContext.ServeTCPPort > 0 {
		addr := fmt.Sprintf(":%d", s.migrationContext.ServeTCPPort)
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			return fmt.Errorf("failed to create TCP listener: %w", err)
		}
		s.tcpListener = listener

		go s.acceptConnections(listener)
		s.migrationContext.Log.Info("Serving on TCP port: %d", s.migrationContext.ServeTCPPort)
	}

	return nil
}

// Stop stops the control server
func (s *Server) Stop() {
	close(s.stopChan)

	if s.unixListener != nil {
		s.unixListener.Close()
	}
	if s.tcpListener != nil {
		s.tcpListener.Close()
	}

	// Clean up Unix socket file
	if s.migrationContext.ServeSocketFile != "" {
		os.Remove(s.migrationContext.ServeSocketFile)
	}
}

// acceptConnections accepts and handles incoming connections
func (s *Server) acceptConnections(listener net.Listener) {
	for {
		select {
		case <-s.stopChan:
			return
		default:
		}

		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-s.stopChan:
				return
			default:
				continue
			}
		}

		go s.handleConnection(conn)
	}
}

// handleConnection handles a single client connection
func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		command := strings.TrimSpace(scanner.Text())
		if command == "" {
			continue
		}

		response := s.handleCommand(command)
		fmt.Fprintln(conn, response)
	}
}

// handleCommand processes a command and returns a response
func (s *Server) handleCommand(command string) string {
	parts := strings.SplitN(command, "=", 2)
	cmd := strings.ToLower(strings.TrimSpace(parts[0]))

	switch cmd {
	case "help":
		return s.helpMessage()

	case "status", "sup":
		return s.printStatus()

	case "chunk-size":
		if len(parts) == 2 {
			value, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
			if err != nil {
				return fmt.Sprintf("ERROR: invalid chunk-size: %v", err)
			}
			if value < 100 {
				value = 100
			} else if value > 100000 {
				value = 100000
			}
			atomic.StoreInt64(&s.migrationContext.ChunkSize, value)
			return fmt.Sprintf("chunk-size=%d", value)
		}
		return fmt.Sprintf("chunk-size=%d", atomic.LoadInt64(&s.migrationContext.ChunkSize))

	case "dml-batch-size":
		if len(parts) == 2 {
			value, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
			if err != nil {
				return fmt.Sprintf("ERROR: invalid dml-batch-size: %v", err)
			}
			if value < 1 {
				value = 1
			} else if value > 1000 {
				value = 1000
			}
			atomic.StoreInt64(&s.migrationContext.DMLBatchSize, value)
			return fmt.Sprintf("dml-batch-size=%d", value)
		}
		return fmt.Sprintf("dml-batch-size=%d", atomic.LoadInt64(&s.migrationContext.DMLBatchSize))

	case "max-lag-millis":
		if len(parts) == 2 {
			value, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
			if err != nil {
				return fmt.Sprintf("ERROR: invalid max-lag-millis: %v", err)
			}
			if value < 0 {
				value = 0
			}
			atomic.StoreInt64(&s.migrationContext.MaxLagMillisecondsThrottleThreshold, value)
			return fmt.Sprintf("max-lag-millis=%d", value)
		}
		return fmt.Sprintf("max-lag-millis=%d", s.migrationContext.MaxLagMillisecondsThrottleThreshold)

	case "nice-ratio":
		if len(parts) == 2 {
			value, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
			if err != nil {
				return fmt.Sprintf("ERROR: invalid nice-ratio: %v", err)
			}
			if value < 0 {
				value = 0
			} else if value > 100 {
				value = 100
			}
			s.migrationContext.NiceRatio = value
			return fmt.Sprintf("nice-ratio=%.2f", value)
		}
		return fmt.Sprintf("nice-ratio=%.2f", s.migrationContext.NiceRatio)

	case "throttle":
		s.migrationContext.SetThrottleControlReasonHint()
		return "throttle=on"

	case "no-throttle":
		s.migrationContext.ClearThrottleControlReasonHint()
		return "throttle=off"

	case "unpostpone":
		// Signal to proceed with cutover
		atomic.StoreInt64(&s.migrationContext.CutOverCompleteFlag, 0)
		return "unpostponed"

	case "panic":
		s.migrationContext.PanicAbort <- fmt.Errorf("panic command received")
		return "panic initiated"

	default:
		return fmt.Sprintf("ERROR: unknown command: %s", cmd)
	}
}

// helpMessage returns the help text
func (s *Server) helpMessage() string {
	return `pg-ost interactive commands:

status, sup         - Show current migration status
help                - Show this help message

chunk-size=N        - Set chunk size (100-100000)
dml-batch-size=N    - Set DML batch size (1-1000)
max-lag-millis=N    - Set max replication lag threshold
nice-ratio=N        - Set nice ratio (0-100, 0=aggressive)

throttle            - Pause migration
no-throttle         - Resume migration
unpostpone          - Proceed with cutover (if postponed)

panic               - Abort migration immediately
`
}
