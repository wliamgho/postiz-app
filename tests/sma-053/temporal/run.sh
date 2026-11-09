#!/bin/sh
# Runs on coder-dsh. Disposable Temporal dev server and PostgreSQL with network
# none; the runner shares only their network namespace and mounts the bundle
# read-only. Everything is removed afterwards.
set -u
DIR=/tmp/sma053-guard-temporal
NET=sma053-guard-tnet
PG=sma053-guard-tpg
RUN=sma053-guard-trun
docker rm -f "$NET" "$PG" "$RUN" >/dev/null 2>&1
docker run -d --name "$NET" --network none --entrypoint temporal temporalio/temporal:latest \
  server start-dev --ip 127.0.0.1 --headless --search-attribute postId=Keyword >/dev/null
docker run -d --name "$PG" --network "container:$NET" --tmpfs /var/lib/postgresql/data \
  -e POSTGRES_PASSWORD=test -e POSTGRES_DB=guard postgres:16-alpine >/dev/null
i=0
until docker exec "$PG" pg_isready -h 127.0.0.1 -U postgres -d guard >/dev/null 2>&1; do
  i=$((i+1)); [ $i -gt 60 ] && echo "postgres not ready" && break; sleep 1
done
sleep 4
docker run --rm --name "$RUN" --network "container:$NET" -v "$DIR:/bundle:ro" \
  --entrypoint sh sma053-isolated-runner:with-workflow -c '
    set -e
    mkdir -p /test/sma053 && cp -r /bundle/bundle /bundle/test.cjs /test/sma053/
    cd /test && node sma053/test.cjs'
STATUS=$?
docker rm -f "$PG" "$NET" >/dev/null 2>&1
rm -rf "$DIR"
echo "exit=$STATUS"
