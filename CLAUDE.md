# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Context restoration

Read `DESIGN.md` at the start of any session. It contains the complete model, SQL schema,
full method-signature inventory, and all critical implementation patterns. Do not re-read all
source files — `DESIGN.md` is the authoritative summary.

## Build commands

```bash
mvn install          # compile, test, package, install to local repo (preferred)
mvn compile          # compile only
mvn test             # compile + run tests
mvn package          # produce target/crawler-if-db-1.0-SNAPSHOT.jar
mvn package -DskipTests   # skip tests if none exist yet
```

No tests have been written yet. Run `mvn install` as the verification step after
any change (or `mvn compile` for a quick syntax check).

## Code generation workflow

This library is **model-driven**. The authoritative source of truth for schema and structure is:

1. **`model.xml`** — declares the database (`<db>`), types (`<type>`), fields, and indexes
2. **`hsqldb.txt`** — the generation spec that defines exactly how to turn the model into Java.
   Always use `hsqldb.txt` as the spec, even if versioned copies (e.g. `hsqldb1.txt`) are
   present. Those are emitted by the generation run for the user to review and swap in; file
   management happens outside the generation run.

If the model changes, the correct approach is to regenerate the affected classes from scratch
using `hsqldb.txt`. `archived/` holds earlier drafts. Do not hand-patch generated code for
schema changes — regenerate.

**Cleanup on regeneration**: Before generating, compare the types in the new `model.xml`
against the file list in `generated-inventory.txt`. Delete the bean, cursor, and persist
sections for any type no longer present. After generation, update `generated-inventory.txt`
to reflect the new file set.

## Architecture

### Three-layer package structure

```
com.gerken.db.bean    — Cargo beans (data holders, no SQL)
com.gerken.db.cursor  — Result-set iterators (read-only, forward-only)
com.gerken.db.persist — PersistCrawlerIf (connection management + all CRUD)
```

**Beans** are pure data: two constructors (`(pk...)` and `(JSONObject)`), getters/setters,
`asJson()`. PK fields are `final` with no setter.

**Cursors** wrap a live `PreparedStatement` + `ResultSet`. They obtain their connection from
`PersistCrawlerIf.getConnection()` (the static `activeConnection` set by the last `open()`
call). Because cursors are in a different package, they cannot access `PersistCrawlerIf`'s
private static `fromRs` helpers — bean-reading logic is inlined in each cursor's `next()`.

**PersistCrawlerIf** is the single entry point. It manages a ref-counted static connection
map (`connectionMap` + `refCountMap`) so multiple `open()` callers sharing the same path
reuse one HSQLDB connection. `open()` and `close()` are `synchronized`.

### Per-type structure inside PersistCrawlerIf

Each model type gets:
- Four cached `PreparedStatement` fields (`psTypeInsert`, `psTypeUpsert`, `psTypeDelete`, `psTypeGet`)
- Four private lazy-getter methods that check `== null || isClosed()` before (re)preparing
- Private static `bindType()` and `typeFromRs()` helpers
- Nine public methods: `createType`, `dropType`, `selectType`, `insert`×3, `delete`×3 (including `deleteType(pk...)`), `upsert`×3, `getType`

`dropType()` must null out all four cached statements **before** the `DROP TABLE`, or HSQLDB
throws because the statements still reference the dropped table.

### Connection URL

```
jdbc:hsqldb:file:<path>/db;shutdown=false
```

`shutdown=false` is intentional — the connection is closed explicitly via `close()`.

### Backup / restore

`backup(path)` and `restore(path)` each launch one thread per type via a fixed
thread pool (size = number of types). Backup threads open their own connection via
private `newConn()` (instance method using `this.path`) so threads do not share a
`Connection`. Lambdas are cast to `(Callable<Void>)` so checked exceptions surface
through `Future.get()`.
Ordering: submit all tasks → `executor.shutdown()` → `f.get()` loop.
