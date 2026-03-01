# crawler-if-db — Design & Session Context

This file exists so that Claude can fully restore project context at the start of a new
session without re-reading all source files. Read this file first, then consult individual
source files only for the details you need.

---

## What this project is

A Java 11 Maven library providing an **HSQLDB embedded-database persistence layer** for a
web-crawler image-gallery application. It is a pure library — no `main()`, no web layer.
Consumers open the database, call CRUD methods, and close.

**Coordinates**

| attribute    | value           |
|--------------|-----------------|
| groupId      | org.gerken      |
| artifactId   | crawler-if-db   |
| version      | 1.0-SNAPSHOT    |
| basePkg      | com.gerken.db   |

**Dependencies**: `org.hsqldb:hsqldb:2.7.3`, `org.codehaus.jettison:jettison:1.5.4`

---

## Key files

| File | Purpose |
|------|---------|
| `model.xml` | Source model — defines the database schema (authoritative) |
| `hsqldb.txt` | The generation spec — always use this for regeneration |
| `generated-inventory.txt` | List of generated Java files — use for cleanup before regenerating |
| `pom.xml` | Maven build file |
| `src/main/java/com/gerken/db/bean/` | Cargo bean classes |
| `src/main/java/com/gerken/db/cursor/` | Cursor classes |
| `src/main/java/com/gerken/db/persist/PersistCrawlerIf.java` | Main persistence class |
| `model.html` | Developer API reference (generated, overwrite each run) |
| `archived/` | Earlier drafts — ignore |

**Model detail**: The model is defined in `model.xml` and documented in `model.html`.
The domain model summary below is kept here for fast LLM context restore.

---

## Domain model — types and fields

### Poster
The owner and producer of one or more galleries.

| Field | Java type | PK | Indexed | Description |
|-------|-----------|----|---------|-------------|
| name  | String    | yes | yes (PK) | The poster's user id |
| url   | String    | no  | no       | The URL of the primary page of the poster's galleries |
| color | String    | no  | yes      | W (whitelisted), B (blacklisted) or G (no decision yet, default) indicating crawl priorities for newly posted galleries |

### Gallery
A collection of images posted by a user and linked to by some number of gallery pages.

| Field      | Java type | PK | Indexed  | Description |
|------------|-----------|----|----------|-------------|
| id         | String    | yes | yes (PK) | The gallery id |
| poster     | String    | no  | yes      | The poster's user name |
| name       | String    | no  | no       | The gallery name |
| url        | String    | no  | no       | The URL of the gallery page |
| attrs      | String    | no  | no       | \|-separated list of gallery categories and tags |
| images     | Integer   | no  | no       | Number of images in the gallery |
| pages      | Integer   | no  | no       | Number of pages in the gallery |
| pagesSeen  | Integer   | no  | no       | Number of gallery pages that have been processed |
| quality    | String    | no  | yes      | Image size. One of S, M, L, XL |
| added      | String    | no  | yes      | When this gallery was added to the site (or modified), in yyyy-mm-dd format |
| state      | String    | no  | yes      | Process state for crawl: ID (identified as new from gallery page results list), CA (categorized after crawling its first gallery page), NA (navigating through all of its gallery pages), IN (inventoried after completing crawl of all gallery pages) |

### Image
An image in a gallery.

| Field     | Java type | PK | Indexed  | Description |
|-----------|-----------|----|----------|-------------|
| pageId    | String    | yes | yes (PK) | The image page id |
| galleryId | String    | no  | yes      | The gallery id |
| pageUrl   | String    | no  | yes      | The URL of the image page |
| imageId   | String    | no  | no       | The image id. Should be the same as the id of the image page that displays the image |
| imageUrl  | String    | no  | yes      | The URL of the image |
| filename  | String    | no  | no       | The local file name (not the absolute path) of the image |
| score     | Integer   | no  | yes      | A user-provided ranking of the image: unscored=-1, poor=0, ok=1, good=2 |
| state     | String    | no  | yes      | Process state for crawl: GP (image discovered on gallery page), IP (information gathered from image page), DL (image downloaded) |

### GalleryAttr
A string indicating the subject matter or context of the images in a gallery. Can be used to automatically prioritize crawl navigation.

| Field | Java type | PK | Indexed  | Description |
|-------|-----------|----|----------|-------------|
| name  | String    | yes | yes (PK) | The attribute itself |
| good  | Boolean   | no  | yes      | Whether there is a bias in favor of galleries with this attribute |
| bad   | Boolean   | no  | yes      | Whether there is a bias against galleries with this attribute |

---

## Generated class inventory

See `generated-inventory.txt` for the exact list of generated files from the most recent
run. Before regenerating, compare that list against the new `model.xml` to identify files
that need to be deleted (types removed from the model).

---

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
- Public methods: `createType`, `dropType`, `selectType`, `insert`×3, `delete`×3
  (including `deleteType(pk...)`), `upsert`×3, `getType`

`dropType()` must null out all four cached statements **before** the `DROP TABLE`, or HSQLDB
throws because the statements still reference the dropped table.

### Connection URL

```
jdbc:hsqldb:file:<path>/db;shutdown=false
```

`shutdown=false` is intentional — the connection is closed explicitly via `close()`.

### Backup / restore

`backup(path)` and `restore(path)` each launch one thread per type via a fixed thread pool.
Backup threads open their own connection via private `newConn()` (instance method using
`this.path`) so threads do not share a `Connection`. Lambdas are cast to `(Callable<Void>)`
so checked exceptions surface through `Future.get()`.
Ordering: submit all tasks → `executor.shutdown()` → `f.get()` loop.

---

## Critical implementation patterns

### NULL handling for non-String types
Only Integer/Long/Boolean/Double fields need a null-safe bind helper:
```java
private static void setIntOrNull(PreparedStatement ps, int idx, Integer v) throws SQLException {
    if (v == null) ps.setNull(idx, Types.INTEGER); else ps.setInt(idx, v);
}
private static void setBoolOrNull(PreparedStatement ps, int idx, Boolean v) throws SQLException {
    if (v == null) ps.setNull(idx, Types.BOOLEAN); else ps.setBoolean(idx, v);
}
```
`ps.setString(idx, null)` works correctly for String fields — no helper needed.

On read, `wasNull()` must be called **immediately** after `getInt()`/`getBoolean()`/etc.,
before any other ResultSet method:
```java
int raw = rs.getInt("score"); bean.setScore(rs.wasNull() ? null : raw);
boolean rawGood = rs.getBoolean("good"); bean.setGood(rs.wasNull() ? null : rawGood);
```

### Lazy PreparedStatement getters
```java
private PreparedStatement galleryInsertStmt() throws SQLException {
    if (psGalleryInsert == null || psGalleryInsert.isClosed())
        psGalleryInsert = conn.prepareStatement("INSERT INTO Gallery ...");
    return psGalleryInsert;
}
```
Check both `null` and `isClosed()`. The `drop*()` methods null out all four cached
statements for that type **before** executing `DROP TABLE IF EXISTS`.

### HSQLDB MERGE (upsert) syntax
```sql
MERGE INTO TypeName
  USING (VALUES (?, ...)) AS t(col1, col2, ...)
  ON TypeName.pk = t.pk
  WHEN MATCHED THEN UPDATE SET nonPk1 = t.nonPk1, ...
  WHEN NOT MATCHED THEN INSERT (col1, ...) VALUES (t.col1, ...)
```
`USING (VALUES ...)` lists **all** columns. `ON` uses only PK columns. `UPDATE SET` uses
only non-PK columns.

### Column ordering rule
All of the following must use the same column order (model-declaration order, PK first):
CREATE TABLE → SELECT → INSERT → MERGE AS t(...) → `bind*()` index assignments → `fromRs()`/`next()` reads.

### Indexable PK fields
If a PK field is marked `indexable="true"` in the model, do **not** create a separate
`CREATE INDEX` statement for it — the `PRIMARY KEY` constraint already provides an index.
Only non-PK fields marked `indexable="true"` get explicit `CREATE INDEX` statements.

### Single-row insert / upsert
Single-row variants use `ps.executeUpdate()` directly; they do not delegate to the
batch variant:
```java
public int insert(Poster row) throws SQLException {
    PreparedStatement ps = posterInsertStmt();
    bindPoster(ps, row);
    return ps.executeUpdate();
}
```
Array-only variants delegate to the offset+len core batch variant.

### Backup/restore thread pattern
Each backup thread uses its own connection (private instance `newConn()`) so threads do
not share a `Connection`. Lambdas are cast to `(Callable<Void>)` so checked exceptions
propagate through `Future.get()`. Thread pool size = number of types in the model.
```java
futures.add(executor.submit((Callable<Void>) () -> {
    try (Connection c = newConn(); ...) { ... }
    return null;
}));
executor.shutdown();
for (Future<?> f : futures) f.get();
```
Restore guards empty arrays before batch upsert: `if (beans.length > 0) upsert(beans);`

### Cursor count() null-safety
```java
String filtered = clauses == null ? "" :
    clauses.replaceAll("(?i)\\s*ORDER\\s+BY\\s+[^;]*", "").trim();
if (!filtered.isBlank()) sql += " " + filtered;
return rs.next() ? rs.getInt(1) : 0;
```

### asJson() for nullable non-String fields
```java
json.put("images", images == null ? JSONObject.NULL : images);
json.put("good",   good   == null ? JSONObject.NULL : good);
```
`JSONObject.put(String, Object)` accepts boxed types; `JSONObject.NULL` avoids NPE.

### prepare() uses remove(), not get()
```java
PreparedStatement ps = clientCache.remove(sql);   // atomically take from cache
if (ps != null && !ps.isClosed()) { clientSqlMap.put(ps, sql); return ps; }
ps = conn.prepareStatement(sql);
clientSqlMap.put(ps, sql);
return ps;
```

---

## Current state

- Code generated and verified with `mvn install` (BUILD SUCCESS).
- Model source: `model.xml`. API documentation: `model.html`.
- All generated Java classes carry full Javadoc (class, fields, constructors, methods).
- No tests have been written yet. Next steps would be:
  1. Write JUnit tests or a quick integration test using an in-memory HSQLDB URL.
- No foreign keys, no composite indexes, no multi-column PKs in the current model.
- `archived/` directory contains earlier drafts — safe to ignore.
- Each generation run emits an updated spec as a versioned file (e.g. hsqldb1.txt);
  the user swaps that in to replace hsqldb.txt externally.
- **Gallery.state** and **Image.state** are both `String` (VARCHAR) — changed from
  `Integer` in this generation run. Gallery states: ID, CA, NA, IN.
  Image states: GP, IP, DL.
