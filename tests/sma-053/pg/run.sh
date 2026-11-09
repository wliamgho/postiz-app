#!/bin/sh
# Runs on coder-dsh. Disposable: network-none PostgreSQL plus a Prisma runner
# sharing only its network namespace; no mounts except the read-only bundle.
set -u
DIR=/tmp/sma053-guard-pg
PG=sma053-guard-pg
RUN=sma053-guard-run
docker rm -f "$PG" "$RUN" >/dev/null 2>&1
docker run -d --name "$PG" --network none --tmpfs /var/lib/postgresql/data \
  -e POSTGRES_PASSWORD=test -e POSTGRES_DB=guard postgres:16-alpine >/dev/null
i=0
until docker exec "$PG" pg_isready -U postgres -d guard >/dev/null 2>&1; do
  i=$((i+1)); [ $i -gt 60 ] && echo "postgres not ready" && break; sleep 1
done
docker run --rm --name "$RUN" --network "container:$PG" \
  -v "$DIR:/bundle:ro" -e DATABASE_URL=postgresql://postgres:test@127.0.0.1:5432/guard \
  --entrypoint sh sma053-prisma-isolated:6.5.0 -c '
    set -e
    cp /bundle/schema.prisma /bundle/test.cjs /bundle/repo-method.js /test/
    cd /test
    npx --no-install prisma generate --schema schema.prisma >/dev/null
    npx --no-install prisma db push --schema schema.prisma --skip-generate >/dev/null
    node test.cjs'
STATUS=$?
docker rm -f "$PG" >/dev/null 2>&1
rm -rf "$DIR"
echo "exit=$STATUS"
