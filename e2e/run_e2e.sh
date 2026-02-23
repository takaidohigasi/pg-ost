#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== pg-ost E2E Test Runner ==="

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null && ! command -v docker &> /dev/null; then
    echo "Error: docker-compose or docker is required"
    exit 1
fi

# Use docker compose (v2) if available, otherwise docker-compose (v1)
if docker compose version &> /dev/null 2>&1; then
    DOCKER_COMPOSE="docker compose"
else
    DOCKER_COMPOSE="docker-compose"
fi

cd "$SCRIPT_DIR"

# Start PostgreSQL primary and replica
echo "Starting PostgreSQL primary and replica..."
$DOCKER_COMPOSE up -d

# Wait for primary to be ready
echo "Waiting for PostgreSQL primary to be ready..."
for i in {1..30}; do
    if $DOCKER_COMPOSE exec -T postgres-primary pg_isready -U pgost -d pgost_test > /dev/null 2>&1; then
        echo "PostgreSQL primary is ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "Error: PostgreSQL primary did not become ready in time"
        $DOCKER_COMPOSE logs postgres-primary
        $DOCKER_COMPOSE down -v
        exit 1
    fi
    echo "Waiting for primary... ($i/30)"
    sleep 1
done

# Wait for replica to be ready
echo "Waiting for PostgreSQL replica to be ready..."
for i in {1..60}; do
    if $DOCKER_COMPOSE exec -T postgres-replica pg_isready -U pgost -d pgost_test > /dev/null 2>&1; then
        # Verify it's actually in recovery mode (is a replica)
        IS_REPLICA=$($DOCKER_COMPOSE exec -T postgres-replica psql -U pgost -d pgost_test -t -c "SELECT pg_is_in_recovery();" 2>/dev/null | tr -d ' \n' || echo "f")
        if [ "$IS_REPLICA" = "t" ]; then
            echo "PostgreSQL replica is ready and in recovery mode!"
            break
        fi
    fi
    if [ $i -eq 60 ]; then
        echo "Warning: PostgreSQL replica did not become ready in time (replica tests will be skipped)"
        $DOCKER_COMPOSE logs postgres-replica
        break
    fi
    echo "Waiting for replica... ($i/60)"
    sleep 2
done

# Run the tests
echo ""
echo "Running e2e tests..."
cd "$PROJECT_DIR"

# Build the binary first
echo "Building pg-ost..."
go build -o bin/pg-ost ./cmd/pg-ost

# Run tests
TEST_RESULT=0
go test -tags=e2e -v -timeout 15m ./e2e/... || TEST_RESULT=$?

# Cleanup
echo ""
echo "Cleaning up..."
cd "$SCRIPT_DIR"
$DOCKER_COMPOSE down -v

if [ $TEST_RESULT -eq 0 ]; then
    echo ""
    echo "=== All e2e tests passed! ==="
else
    echo ""
    echo "=== Some e2e tests failed ==="
    exit $TEST_RESULT
fi
