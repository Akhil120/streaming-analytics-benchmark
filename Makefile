FLINK_REST  := http://localhost:8082
SQL_GATEWAY := http://localhost:8083

.PHONY: up down logs clean status \
        gen-logs gen-pause gen-resume \
        topic-describe consumer-lag \
        ch ch-query ch-ingestion ch-freshness ch-lag \
        check-clocks \
        flink flink-jobs flink-logs flink-sql flink-submit flink-cancel \
        flink-p2 flink-p3 \
        minio \
        ui grafana prometheus smoke-test

# Start all Phase 1 services
up:
	docker compose up -d --build

# Stop all services (keep volumes)
down:
	docker compose down

# Stop all services and wipe volumes (full reset)
clean:
	docker compose down -v

# Follow all logs
logs:
	docker compose logs -f

# Service health overview
status:
	docker compose ps

# Generator throughput stats
gen-logs:
	docker compose logs -f generator

# Pause / resume data generation without losing container state
gen-pause:
	docker compose stop generator
	@echo "Generator paused. Run 'make gen-resume' to restart."

gen-resume:
	docker compose start generator
	@echo "Generator resumed."

# Describe the transactions topic
topic-describe:
	docker compose exec broker kafka-topics --describe \
		--bootstrap-server broker:29092 \
		--topic transactions

# List consumer groups and their lag
consumer-lag:
	docker compose exec broker kafka-consumer-groups --bootstrap-server broker:29092 \
		--list | xargs -I{} docker compose exec broker \
		kafka-consumer-groups --bootstrap-server broker:29092 --describe --group {}

# ── ClickHouse (Phase 2) ──────────────────────────────────────────────────────

# Open interactive ClickHouse client (analytics database)
ch:
	docker compose exec clickhouse clickhouse-client --database analytics

# Run a SQL query inline: make ch-query Q="SELECT count() FROM transactions"
ch-query:
	docker compose exec clickhouse clickhouse-client --database analytics --query "$(Q)"

# Check how many rows have been ingested and the current compression ratio
ch-ingestion:
	docker compose exec clickhouse clickhouse-client --database analytics --query \
		"SELECT table, formatReadableQuantity(sum(rows)) AS rows, \
		formatReadableSize(sum(data_compressed_bytes)) AS compressed \
		FROM system.parts WHERE database='analytics' AND active GROUP BY table"

# Run Q3 freshness probe (freshness_lag_ms, pipeline_lag_ms, total_rows)
ch-freshness:
	docker compose exec clickhouse clickhouse-client --database analytics --query \
		"SELECT max(event_time) AS newest, now64(3) AS now, \
		dateDiff('millisecond', max(event_time), now64(3)) AS freshness_lag_ms, \
		dateDiff('millisecond', max(event_time), max(ingest_time)) AS pipeline_lag_ms, \
		count() AS total_rows FROM analytics.transactions"

# Show Kafka Engine consumer status and any exceptions
ch-lag:
	docker compose exec clickhouse clickhouse-client --query \
		"SELECT consumer_id, num_messages_read, num_rebalance_assignments, \
		num_rebalance_revocations, exceptions.text \
		FROM system.kafka_consumers WHERE database='analytics' FORMAT Vertical"

# ── Flink + Fluss (Phase 3) ───────────────────────────────────────────────────

# Open Flink UI
flink:
	open http://localhost:8082

# List all Flink jobs and their status via REST
flink-jobs:
	curl -s $(FLINK_REST)/jobs | python3 -m json.tool

# Follow JobManager + TaskManager logs
flink-logs:
	docker compose logs -f flink-jobmanager flink-taskmanager

# Open interactive Flink SQL client connected to SQL Gateway (not embedded)
flink-sql:
	docker compose exec flink-jobmanager /opt/flink/bin/sql-client.sh -e http://localhost:8083

# Run P2 benchmark queries (Fluss Union Read — hot + cold) via SQL Gateway
flink-p2:
	docker compose exec -T flink-jobmanager bash -c \
	    'cat /sql/benchmark_p2.sql | /opt/flink/bin/sql-client.sh -e http://localhost:8083'

# Run P3 benchmark queries (Fluss cold lake only — Parquet scan) via SQL Gateway
flink-p3:
	docker compose exec -T flink-jobmanager bash -c \
	    'cat /sql/benchmark_p3.sql | /opt/flink/bin/sql-client.sh -e http://localhost:8083'

# Cancel all RUNNING Flink jobs via REST API
flink-cancel:
	@curl -sf $(FLINK_REST)/jobs | \
	  python3 -c "import sys,json; [print(j['id']) for j in json.load(sys.stdin)['jobs'] if j['status']=='RUNNING']" | \
	  xargs -I{} curl -sf -X PATCH "$(FLINK_REST)/jobs/{}?mode=cancel" || true
	@echo "All running jobs cancelled."

# Submit tiering JAR via Flink REST API (upload + run — no docker exec needed)
flink-submit-tiering:
	@echo "Uploading Fluss tiering JAR..."
	@JAR_ID=$$(docker compose exec -T flink-jobmanager \
	    curl -sf -X POST -F "jarfile=@/opt/flink/opt/fluss-flink-tiering-0.9.1-incubating.jar" \
	    http://localhost:8081/jars/upload | \
	    python3 -c "import sys,json; print(json.load(sys.stdin)['filename'].split('/')[-1])"); \
	echo "Submitting tiering job ($$JAR_ID)..."; \
	docker compose exec -T flink-jobmanager \
	    curl -sf -X POST http://localhost:8081/jars/$$JAR_ID/run \
	    -H "Content-Type: application/json" \
	    -d '{"programArgsList":["--fluss.bootstrap.servers","coordinator-server:9123","--datalake.format","paimon","--datalake.paimon.metastore","filesystem","--datalake.paimon.warehouse","/tmp/paimon/warehouse","--s3.endpoint","http://minio:9000","--s3.access-key","minioadmin","--s3.secret-key","minioadmin","--s3.path-style-access","true","--s3.region","us-east-1","--s3.assumed.role.arn","arn:aws:iam::000000000000:role/minioadmin","--s3.assumed.role.sts.endpoint","http://minio:9000"]}' | \
	    python3 -c "import sys,json; d=json.load(sys.stdin); print('Tiering job ID:', d.get('jobid','ERROR'))"

# Submit Kafka→Fluss SQL ingestion job via SQL Gateway REST API
# Note: pipe via stdin — sql-client -f silently skips in gateway mode
flink-submit-sql:
	docker compose exec -T flink-jobmanager bash -c \
	    'cat /sql/01_kafka_to_fluss.sql | /opt/flink/bin/sql-client.sh -e http://localhost:8083'

# Submit both jobs: tiering JAR + SQL ingestion (cancel existing jobs first)
flink-submit: flink-submit-tiering flink-submit-sql

# Open MinIO console (cold storage for Fluss datalake tier)
minio:
	open http://localhost:9001

# ── Clock skew verification ───────────────────────────────────────────────────
# All containers must agree on UTC wall-clock to within ~5ms for accurate
# freshness_lag_ms and pipeline_lag_ms measurements.
# On macOS Docker Desktop containers share the same VM kernel clock, so
# true drift is zero; this check catches timezone misconfiguration.

check-clocks:
	@echo "UTC wall-clock across containers (should all match to <5ms):"
	@for svc in broker clickhouse generator schema-registry; do \
	    t=$$(docker compose exec -T $$svc date -u +"%Y-%m-%dT%H:%M:%S.%3NZ" 2>/dev/null || echo "unavailable"); \
	    printf "  %-20s %s\n" "$$svc" "$$t"; \
	done

# ── UIs ───────────────────────────────────────────────────────────────────────

# Open UIs (macOS)
ui:
	open http://localhost:8090

grafana:
	open http://localhost:3000

prometheus:
	open http://localhost:9090

# Produce a single test message to verify setup
smoke-test:
	echo '{"event_id":"test-1","user_id":1,"amount":9.99,"region":"us-east-1","event_type":"purchase","event_time":"2025-01-01T00:00:00.000Z"}' | \
	docker compose exec -T broker kafka-console-producer \
		--bootstrap-server broker:29092 \
		--topic transactions
