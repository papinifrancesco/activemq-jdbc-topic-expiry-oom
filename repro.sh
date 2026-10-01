#!/usr/bin/env bash
# Reproduces: the periodic topic expiry task of Apache ActiveMQ loads the WHOLE JDBC topic
# store into the heap when the store is PostgreSQL (see README.md).
#
# Needs: bash, curl, tar, xz, a JDK 17+ in JAVA_HOME (for jcmd), a reachable PostgreSQL
# (only for STORE=jdbc). Everything else is downloaded and checksum-verified.
set -euo pipefail

AMQ_VERSION=${AMQ_VERSION:-6.2.10}
STORE=${STORE:-jdbc}                  # jdbc | kahadb
EXPIRE_PERIOD=${EXPIRE_PERIOD:-default}  # default (broker default, 30000 ms) | milliseconds, e.g. 0
EXPECT=${EXPECT:-oom}                 # oom | survive
HEAP=${HEAP:-512M}
THREADS=${THREADS:-4}                 # producer threads
PER_THREAD=${PER_THREAD:-40000}       # messages per producer thread
MSG_SIZE=${MSG_SIZE:-5000}            # bytes per message
PGHOST=${PGHOST:-127.0.0.1}; PGPORT=${PGPORT:-5432}
PGDATABASE=${PGDATABASE:-repro}; PGUSER=${PGUSER:-repro}; PGPASSWORD=${PGPASSWORD:-repro}
PSQL=${PSQL:-psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE}  # how to run SQL (row counts)
WORK=${WORK:-$PWD/work}; OUT=${OUT:-$PWD/out}

PGJDBC_VERSION=42.7.13
PGJDBC_SHA256=6e0e4cc2d8cae902084f8a2b18728b073a6fd9d1f87c9d8bff8f298c18185b93
TOPIC=repro.topic
BROKER_URL=tcp://127.0.0.1:61616
: "${JAVA_HOME:?set JAVA_HOME to a JDK 17+}"
export PGPASSWORD

log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$OUT/repro.log"; }
sql() { eval "$PSQL -tAc \"$1\"" 2>/dev/null | tr -d '[:space:]' || true; }

rm -rf "$OUT"; mkdir -p "$WORK" "$OUT"
log "ActiveMQ $AMQ_VERSION, store=$STORE, expireMessagesPeriod=$EXPIRE_PERIOD, expect=$EXPECT, heap=$HEAP"
log "load: $THREADS x $PER_THREAD persistent messages of $MSG_SIZE bytes, no TTL, one OFFLINE durable subscriber"

# --- 1. ActiveMQ binary distribution, verified against the published sha512 ------------------
TGZ="apache-activemq-$AMQ_VERSION-bin.tar.gz"
BASE="https://archive.apache.org/dist/activemq/$AMQ_VERSION"
[ -f "$WORK/$TGZ" ] || curl -fsSL -o "$WORK/$TGZ" "$BASE/$TGZ"
want=$(curl -fsSL "$BASE/$TGZ.sha512" | grep -oE '[0-9a-f]{128}' | head -1)
have=$(sha512sum "$WORK/$TGZ" | cut -d' ' -f1)
[ "$want" = "$have" ] || { log "sha512 mismatch for $TGZ"; exit 2; }
AMQ="$WORK/apache-activemq-$AMQ_VERSION"
rm -rf "$AMQ"; tar -xzf "$WORK/$TGZ" -C "$WORK"

# --- 2. PostgreSQL JDBC driver, verified ----------------------------------------------------
JAR="$WORK/postgresql-$PGJDBC_VERSION.jar"
[ -f "$JAR" ] || curl -fsSL -o "$JAR" \
  "https://repo1.maven.org/maven2/org/postgresql/postgresql/$PGJDBC_VERSION/postgresql-$PGJDBC_VERSION.jar"
[ "$(sha256sum "$JAR" | cut -d' ' -f1)" = "$PGJDBC_SHA256" ] || { log "sha256 mismatch for pgjdbc"; exit 2; }
cp "$JAR" "$AMQ/lib/optional/"

# --- 3. broker configuration ------------------------------------------------------------------
if [ "$STORE" = jdbc ]; then
  PERSISTENCE='<jdbcPersistenceAdapter dataSource="#pg-ds" createTablesOnStartup="true" useLock="false"/>'
  sql "DROP TABLE IF EXISTS activemq_msgs, activemq_acks, activemq_lock" >/dev/null
else
  PERSISTENCE="<kahaDB directory=\"$AMQ/data/kahadb\"/>"
fi
EXPIRE_ATTR=""; [ "$EXPIRE_PERIOD" = default ] || EXPIRE_ATTR="expireMessagesPeriod=\"$EXPIRE_PERIOD\""
sed -e "s|@PERSISTENCE@|$PERSISTENCE|" -e "s|@EXPIRE_ATTR@|$EXPIRE_ATTR|" \
    -e "s|@PGURL@|jdbc:postgresql://$PGHOST:$PGPORT/$PGDATABASE|" -e "s|@PGUSER@|$PGUSER|" \
    -e "s|@PGPASSWORD@|$PGPASSWORD|" -e "s|@DATA@|$AMQ/data|" activemq.xml.template > "$WORK/activemq.xml"
cp "$WORK/activemq.xml" "$OUT/activemq.xml"

# --- 4. start the broker with a STRIPPED environment (the heap dump must hold nothing else) ----
mkdir -p "$WORK/home"
env -i PATH=/usr/bin:/bin HOME="$WORK/home" JAVA_HOME="$JAVA_HOME" \
  ACTIVEMQ_OPTS_MEMORY="-Xms64M -Xmx$HEAP -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$OUT" \
  "$AMQ/bin/activemq" console "xbean:file:$WORK/activemq.xml" > "$OUT/broker-console.log" 2>&1 &
for _ in $(seq 1 90); do (exec 3<>/dev/tcp/127.0.0.1/61616) 2>/dev/null && break; sleep 2; done
PID=$(pgrep -f "activemq\.jar.*xbean:file:$WORK/activemq.xml" | head -1 || true)
[ -n "$PID" ] || { log "broker did not start"; cat "$OUT/broker-console.log"; exit 2; }
log "broker up, pid $PID"

cli() { ACTIVEMQ_OPTS_MEMORY="-Xmx256M" "$AMQ/bin/activemq" "$@"; }

# --- 5. one durable subscriber, then disconnect it (offline: the backlog stays in the store) ---
timeout 120 cli consumer --brokerUrl "$BROKER_URL" --destination "topic://$TOPIC" \
  --durable true --clientId repro-client --messageCount 0 > "$OUT/consumer.log" 2>&1
log "offline durable subscription created"

# --- 6. sampler: store rows, heap use, and the first thread dump showing doRecover -------------
printf 'utc\tstore_rows\theap_used_kb\n' > "$OUT/samples.tsv"
(
  while kill -0 "$PID" 2>/dev/null; do
    rows=-; [ "$STORE" = jdbc ] && rows=$(sql "SELECT count(*) FROM activemq_msgs")
    used=$(timeout 10 "$JAVA_HOME/bin/jcmd" "$PID" GC.heap_info 2>/dev/null | grep -oE 'used [0-9]+K' | head -1 | tr -dc 0-9)
    printf '%s\t%s\t%s\n' "$(date -u +%H:%M:%S)" "${rows:--}" "${used:--}" >> "$OUT/samples.tsv"
    if [ ! -f "$OUT/thread-dump-doRecover.txt" ] && \
       timeout 15 "$JAVA_HOME/bin/jcmd" "$PID" Thread.print > "$OUT/.td" 2>/dev/null && \
       grep -q 'DefaultJDBCAdapter.doRecover' "$OUT/.td"; then
      mv "$OUT/.td" "$OUT/thread-dump-doRecover.txt"
    fi
    sleep 5
  done
) &
SAMPLER=$!

# --- 7. load ---------------------------------------------------------------------------------
cli producer --brokerUrl "$BROKER_URL" --destination "topic://$TOPIC" --persistent true \
  --messageCount "$PER_THREAD" --messageSize "$MSG_SIZE" --parallelThreads "$THREADS" \
  > "$OUT/producer.log" 2>&1 &
PRODUCER=$!
oom() { grep -q 'java.lang.OutOfMemoryError' "$OUT/broker-console.log" "$AMQ/data/activemq.log" 2>/dev/null \
        || ls "$OUT"/*.hprof >/dev/null 2>&1; }
deadline=$(( $(date +%s) + 1200 ))
while kill -0 "$PRODUCER" 2>/dev/null && ! oom && [ "$(date +%s)" -lt "$deadline" ]; do sleep 5; done
if ! oom; then
  log "producer finished; waiting 3 expiry periods (90 s)"
  for _ in $(seq 1 18); do oom && break; sleep 5; done
fi
sleep 10                                   # let the heap dump finish and the WARN reach the log
PRODUCED=$(grep -c 'Produced: ' "$OUT/producer.log" || true)
ROWS=-; [ "$STORE" = jdbc ] && ROWS=$(sql "SELECT count(*) FROM activemq_msgs")
ALIVE=no; kill -0 "$PID" 2>/dev/null && ALIVE=yes

# --- 8. evidence -------------------------------------------------------------------------------
cp "$AMQ/data/activemq.log" "$OUT/activemq.log" 2>/dev/null || true
grep -h -A 40 -E 'Failed to (browse|expire messages on) Topic' "$OUT/activemq.log" "$OUT/broker-console.log" \
  2>/dev/null | head -60 > "$OUT/oom-warn.txt" || true
STACK=no
grep -q 'DefaultJDBCAdapter.doRecover' "$OUT/oom-warn.txt" "$OUT/thread-dump-doRecover.txt" 2>/dev/null && STACK=yes
OOM=no; oom && OOM=yes
kill "$SAMPLER" "$PRODUCER" 2>/dev/null || true; kill -9 "$PID" 2>/dev/null || true
for f in "$OUT"/*.hprof; do if [ -f "$f" ]; then xz -T0 -6 "$f"; fi; done
rm -f "$OUT/.td"

PASS=no
if [ "$EXPECT" = oom ]; then [ "$OOM" = yes ] && [ "$STACK" = yes ] && PASS=yes
else [ "$OOM" = no ] && [ "$ALIVE" = yes ] && PASS=yes; fi

{
  echo "### ActiveMQ $AMQ_VERSION, store=$STORE, expireMessagesPeriod=$EXPIRE_PERIOD"
  echo
  echo "| | |"; echo "|---|---|"
  echo "| expected | $EXPECT |"
  echo "| OutOfMemoryError | $OOM |"
  echo "| doRecover on the OOM path (WARN stack or thread dump) | $STACK |"
  echo "| broker alive at the end | $ALIVE |"
  echo "| producer 'Produced:' lines | $PRODUCED |"
  echo "| rows in activemq_msgs at the end | $ROWS |"
  echo "| heap | $HEAP |"
  echo "| **result** | **$( [ $PASS = yes ] && echo PASS || echo FAIL )** |"
  echo
  if [ -s "$OUT/oom-warn.txt" ]; then echo '```'; head -30 "$OUT/oom-warn.txt"; echo '```'; fi
} > "$OUT/summary.md"
cat "$OUT/summary.md"
[ "$PASS" = yes ]
