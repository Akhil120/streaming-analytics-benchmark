#!/bin/bash
# Custom JM entrypoint: starts JobManager + SQL Gateway in the same container.
# SQL Gateway (port 8083) provides a REST API for SQL statement submission,
# replacing embedded sql-client mode so jobs run on the JM+TM cluster.

# Start the JobManager. docker-entrypoint.sh processes FLINK_PROPERTIES into
# config.yaml before launching the JM process.
/docker-entrypoint.sh jobmanager &
JM_PID=$!

# Wait for the JM REST API to be up before starting the SQL Gateway.
# config.yaml is written by docker-entrypoint.sh above, so the gateway
# will find the correct jobmanager.rpc.address on startup.
until curl -sf http://localhost:8081/overview >/dev/null 2>&1; do sleep 2; done
echo "JobManager ready — starting SQL Gateway on :8083"

# Run SQL Gateway in foreground so the container stays alive.
exec /opt/flink/bin/sql-gateway.sh start-foreground
