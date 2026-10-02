# ActiveMQ: topic expiry task loads the whole JDBC topic store into the heap (PostgreSQL)

Reproduction for an Apache ActiveMQ bug. When the broker uses the **JDBC persistence adapter on PostgreSQL**, the periodic topic expiry task (`expireMessagesPeriod`, default 30 s) reads **every stored message of the topic** into the heap. That is the whole backlog of an offline or slow durable subscriber. A large enough backlog makes the broker run out of memory, even though no message has a time-to-live.

- Affected: 6.2.10 and 6.3.2 (both tested here). The code path has been the same since [AMQ-9698](https://issues.apache.org/jira/browse/AMQ-9698) (6.2.0).
- Related: [AMQ-6067](https://issues.apache.org/jira/browse/AMQ-6067), the same symptom on Oracle, fixed in 5.14.1 by stopping the recovery loop after `maxExpirePageSize` rows. That fix cannot help on PostgreSQL: the driver buffers the full result set inside `executeQuery()`, before the loop sees the first row.
- Issue: [apache/activemq#2630](https://github.com/apache/activemq/issues/2630)
- A proposed fix is in [`patches/`](patches/) and is tested by this repository (see [Proposed fix](#proposed-fix)). While writing it, a second JDBC bug turned up: expiring a message can silently drop **earlier, non-expired** messages of the same durable subscriber.

## The code path

1. `Topic.expireMessagesTask` uses `store.recoverExpired(...)` only when `store.getType() == StoreType.KAHADB`. Any other store falls back to `doBrowse(new InsertionCountList<>(), getMaxExpirePageSize())`. In 6.3.2 that is `Topic.java` lines 930 and 980-982; on `activemq-6.2.x`, lines 898 and 950.
2. `doBrowse` calls `JDBCMessageStore.recover()`, which calls `DefaultJDBCAdapter.doRecover()` (line 399). That runs `SELECT ID, MSG FROM ACTIVEMQ_MSGS WHERE CONTAINER=? ORDER BY ID` with no `setMaxRows()` and no fetch size. The other recovery queries in the same class do call `setMaxRows` (lines 433, 593, 627, 1068).
3. pgjdbc only fetches in batches when autocommit is off **and** a fetch size is set ([pgjdbc docs](https://jdbc.postgresql.org/documentation/query/), "Getting results based on a cursor"). Otherwise it reads every row into memory. The `hasSpace()` check added by AMQ-6067 runs only after that.

## What the reproduction does

`repro.sh`, the same script locally and in CI:

1. Downloads the official binary distribution and checks it against Apache's published sha512. Downloads pgjdbc 42.7.13 and checks its sha256.
2. Starts a broker with a 1 GB heap, the JDBC store on PostgreSQL, default data-source settings and the default `expireMessagesPeriod`. The broker runs with a stripped environment, so the heap dump holds only synthetic data.
3. Registers one durable subscriber with the stock CLI (`activemq consumer --durable true --messageCount 0`), which then disconnects.
4. Publishes 4 × 50,000 persistent 5,000-byte messages (`activemq producer`), with no TTL unless `TTL` is set.
5. Samples the store row count and heap use every 5 s, and takes a thread dump every second to catch the expiry browse in the act. Records whether an `OutOfMemoryError` happens and whether `DefaultJDBCAdapter.doRecover` is on its path (the broker's WARN stack trace or a thread dump). It also records the subscriber's `LAST_ACKED_ID`: the subscriber stays offline, so only the expiry task can move it.

For the patched legs, `build-patched.sh` builds `activemq-broker` and `activemq-jdbc-store` from the release tag with the patch applied, and `repro.sh` puts the two jars over the ones of the official distribution.

The [workflow](.github/workflows/repro.yml) runs eight cases with a PostgreSQL 17 service container:

| ActiveMQ | Store | `expireMessagesPeriod` | TTL | Expected |
|---|---|---|---|---|
| 6.2.10 | JDBC / PostgreSQL | default (30 s) | none | OutOfMemoryError in `doRecover` |
| 6.3.2 | JDBC / PostgreSQL | default (30 s) | none | OutOfMemoryError in `doRecover` |
| 6.3.2 | JDBC / PostgreSQL | default (30 s) | 30 s | OutOfMemoryError in `doRecover` |
| 6.2.10 | JDBC / PostgreSQL | `0` (task disabled) | none | survives the same load |
| 6.2.10 | KahaDB | default (30 s) | none | survives the same load |
| 6.3.2 + patch | JDBC / PostgreSQL | default (30 s) | none | survives the same load |
| 6.3.2 + patch | JDBC / PostgreSQL | default (30 s) | 30 s | survives, and the expiry task acks expired messages (`LAST_ACKED_ID` moves) |
| 6.2.10 + patch | JDBC / PostgreSQL | default (30 s) | none | survives the same load |

A job passes when the expected outcome is observed. Each run uploads its evidence: broker logs, the WARN stack, the thread dump showing `doRecover`, `samples.tsv`, and the heap dump (`.hprof.xz`) when one is written.

## Results

### The bug

From [run 36845523696](https://github.com/papinifrancesco/activemq-jdbc-topic-expiry-oom/actions/runs/36845523696). The evidence is kept permanently in the [`evidence-2026-10-01` release](https://github.com/papinifrancesco/activemq-jdbc-topic-expiry-oom/releases/tag/evidence-2026-10-01).

| ActiveMQ | Store | `expireMessagesPeriod` | Outcome (1 GB heap, 200,000 × 5 KB messages offered) |
|---|---|---|---|
| 6.2.10 | JDBC / PostgreSQL | default | `OutOfMemoryError` at ~86,000 stored rows. `WARN Failed to browse Topic: repro.topic`, browse caught 3× inside `DefaultJDBCAdapter.doRecover` |
| 6.3.2 | JDBC / PostgreSQL | default | `OutOfMemoryError` at ~89,000 stored rows, broker exits. Same stack, caught 3× |
| 6.2.10 | JDBC / PostgreSQL | `0` | all 200,000 rows stored, no OOM, broker up |
| 6.2.10 | KahaDB | default | all messages stored, no OOM, broker up |

Eclipse MAT "Leak Suspects" on the 6.2.10 heap dump (report in the release): the thread `ActiveMQ BrokerService[repro] Task-3` retains **53.8% of the heap (574 MB)** in one `ArrayList` of **54,343 `org.postgresql.core.Tuple`**, one per row. The driver was still receiving the ~86,000-row result set inside `executeQuery()` when the heap ran out. Its stack:

```
at org.postgresql.core.v3.QueryExecutorImpl.processResults(QueryExecutorImpl.java:2623)
at org.postgresql.jdbc.PgPreparedStatement.executeQuery(PgPreparedStatement.java:140)
at org.apache.commons.dbcp2.DelegatingPreparedStatement.executeQuery(DelegatingPreparedStatement.java:123)
at org.apache.activemq.store.jdbc.adapter.DefaultJDBCAdapter.doRecover(DefaultJDBCAdapter.java:406)
at org.apache.activemq.store.jdbc.JDBCMessageStore.recover(JDBCMessageStore.java:279)
at org.apache.activemq.store.ProxyTopicMessageStore.recover(ProxyTopicMessageStore.java:63)
at org.apache.activemq.broker.region.Topic.doBrowse(Topic.java:691)
at org.apache.activemq.broker.region.Topic.lambda$new$2(Topic.java:950)
```

The heap dumps contain only the synthetic messages. The broker runs with a stripped environment, and the database credentials are the throwaway `repro`/`repro`.

### With the proposed fix

From [run 37039551631](https://github.com/papinifrancesco/activemq-jdbc-topic-expiry-oom/actions/runs/37039551631), all eight cases as expected:

| ActiveMQ | TTL | Outcome (1 GB heap, 200,000 × 5 KB messages offered) |
|---|---|---|
| 6.3.2 (unpatched) | 30 s | `OutOfMemoryError` in `doRecover`: a TTL does not help, the browse still loads the whole backlog |
| 6.3.2 + patch | none | all 200,000 rows stored, no OOM, broker up |
| 6.3.2 + patch | 30 s | no OOM, broker up, heap 566 MB after the load. The expiry task acked 2,400 expired messages (`LAST_ACKED_ID` 0 → 2,400), i.e. 400 (`maxExpirePageSize`) per 30 s run. The 2,400 extra rows in `ACTIVEMQ_MSGS` are their copies in `ActiveMQ.DLQ` (default dead letter strategy) |
| 6.2.10 + patch | none | all 200,000 rows stored, no OOM, broker up |

The TTL case is the one that shows the row limit works on PostgreSQL: every pending message is expired, so without the limit one expiry run would read the whole backlog.

## Proposed fix

[`patches/jdbc-recover-expired.patch`](patches/jdbc-recover-expired.patch) (against `main`, applies to 6.3.2) and [`patches/jdbc-recover-expired-6.2.10.patch`](patches/jdbc-recover-expired-6.2.10.patch) (the same change on 6.2.10). It implements `recoverExpired()` for the JDBC store, so the expiry task no longer browses the topic:

- `Topic` uses `recoverExpired()` for JDBC as well as KahaDB.
- `JDBCTopicMessageStore.recoverExpired()` runs, per eligible durable subscriber, two queries: one that reads the subscriber's last acked id and the first pending message that is not expired, then one that loads the expired messages between the two, at most `maxExpirePageSize` of them (`setMaxRows`). Both are plain SQL, so the change is not specific to PostgreSQL.
- A topic browse (JMX, the statistics plugin) expires messages through the same path on JDBC.

**The second bug.** The JDBC store records a durable subscriber's acks as a single high-water mark: `UPDATE ACTIVEMQ_ACKS SET LAST_ACKED_ID=?`. Acking an expired message therefore also acks every earlier message of that subscriber. Today, when a message with a TTL expires behind a message without one, the expiry task (or a browse) acks the expired message and the earlier one is never delivered: after a restart it is gone. KahaDB tracks acks per message and does not have this problem. The patch only expires the run of expired messages that directly follows the last acked id, and stops at the first message that has not expired or belongs to a prepared XA transaction (per priority, with prioritized messages). As a consequence, expired messages queued behind a non-expiring one stay in the store until that one is consumed; they are still discarded at dispatch.

The patch adds unit tests (Derby) for both bugs. One case that is left out: a manual browse of a JDBC topic still reads the whole result set on PostgreSQL. Only an explicit JMX or statistics-plugin browse triggers it, never the periodic task.

## Run it locally

Needs bash, curl, tar, xz, a JDK 17+ in `JAVA_HOME` (for `jcmd`) and a PostgreSQL you can drop tables in:

```bash
PGHOST=127.0.0.1 PGPORT=5432 PGDATABASE=repro PGUSER=repro PGPASSWORD=repro ./repro.sh
AMQ_VERSION=6.3.2 ./repro.sh
EXPIRE_PERIOD=0 EXPECT=survive ./repro.sh
STORE=kahadb EXPECT=survive ./repro.sh

# with the proposed fix (6.3.x needs a JDK 24+ to build, 6.2.x a JDK 17+)
AMQ_VERSION=6.3.2 ./build-patched.sh
AMQ_VERSION=6.3.2 PATCHED=yes EXPECT=survive ./repro.sh
AMQ_VERSION=6.3.2 PATCHED=yes TTL=30000 EXPECT=expire ./repro.sh
```

Other knobs: `HEAP` (default `1G`), `THREADS`, `PER_THREAD`, `MSG_SIZE`. Results go to `out/`, with a summary in `out/summary.md`.

## Workaround

Set `expireMessagesPeriod="0"` on the topic `policyEntry`. Expired messages are still discarded when they are dispatched (`PrefetchSubscription.dispatchPending`). They are no longer removed while a durable subscriber is offline, because the JDBC cleanup (`doDeleteOldMessages`) deletes only acknowledged rows.

## License

Apache License 2.0, see [LICENSE](LICENSE).
