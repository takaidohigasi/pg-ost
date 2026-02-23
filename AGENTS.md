# AGENTS.md - Guide for AI Coding Agents

See [README.md](README.md) for project overview, features, usage, and CLI flags.

## Quick Reference

```bash
make build          # Build binary to bin/pg-ost
make test           # Run unit tests
make test-e2e       # Run E2E tests (requires Docker)
make lint           # Run golangci-lint
make fmt            # Format code
```

## Code Style

### Copyright Header (Required)

All `.go` files must include:

```go
/*
   Copyright 2024
   pg-ost: PostgreSQL Online Schema Transformation
*/
```

### Formatting

- Run `make fmt` and `make lint` before committing
- Import order: stdlib, external, local (`github.com/takaidohigasi/pg-ost`)
- Linter config: `.golangci.yml`

## Testing

### Unit Tests

```bash
go test -v ./internal/...
```

### E2E Tests

```bash
make test-e2e       # Or: ./e2e/run_e2e.sh
```

- Build tag: `//go:build e2e`
- Primary DB: `localhost:15432` (user: pgost, password: pgost, db: pgost_test)
- Replica DB: `localhost:15433` (same credentials)

## Key Directories

| Directory | Purpose |
|-----------|---------|
| `cmd/pg-ost/` | CLI entry point |
| `internal/logic/` | Migration orchestration (migrator, applier, streamer) |
| `internal/base/` | MigrationContext and core types |
| `internal/replication/` | Logical replication components |
| `e2e/` | E2E tests and Docker Compose setup |

## Common Gotchas

1. **Atomic counters**: Use `atomic.AddInt64()` / `atomic.LoadInt64()` for `TotalRowsCopied`, `TotalDMLEventsApplied`, etc.
2. **Channel buffering**: `applyEventsQueue` is buffered (1000), `copyRowsQueue` is unbuffered
3. **Context cancellation**: Check `ctx.PanicAbort` in background goroutines
4. **Connection closing**: Always `defer Close()` on database connections
5. **Table naming**: Ghost=`_{table}_gho`, Changelog=`_{table}_ghc`, Old=`_{table}_old`
6. **REPLICA IDENTITY**: Must be FULL before migration starts
7. **Unique key**: Required (primary key or unique index)

## CI Pipeline

GitHub Actions (`.github/workflows/ci.yml`) runs:
- **lint**: golangci-lint
- **test**: Unit tests with race detection
- **e2e-test**: E2E tests with PostgreSQL primary + replica
- **build**: Cross-platform builds (linux/darwin, amd64/arm64)

## When to Update README.md

Update README.md when:
- Adding new CLI flags or changing defaults
- Adding new features that users need to know about
- Changing installation instructions
- Modifying the migration workflow (How It Works section)
- Adding new online commands or hooks
- Changing PostgreSQL version requirements

Do NOT update README.md for:
- Internal refactoring that doesn't change user-facing behavior
- Bug fixes (unless they change documented behavior)
- Test additions or changes
- CI/CD changes
