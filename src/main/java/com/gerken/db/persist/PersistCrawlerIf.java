package com.gerken.db.persist;

import com.gerken.db.bean.Caption;
import com.gerken.db.bean.GalleryAttr;
import com.gerken.db.bean.Gallery;
import com.gerken.db.bean.Image;
import com.gerken.db.bean.Poster;
import com.gerken.db.cursor.CaptionCursor;
import com.gerken.db.cursor.GalleryAttrCursor;
import com.gerken.db.cursor.GalleryCursor;
import com.gerken.db.cursor.ImageCursor;
import com.gerken.db.cursor.PosterCursor;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.codehaus.jettison.json.JSONObject;

/**
 * Single entry point for the crawler-if HSQLDB persistence layer.
 *
 * <p>Manages an embedded HSQLDB file database and provides full CRUD access to the
 * five model types: {@link Poster}, {@link Gallery}, {@link Image}, {@link GalleryAttr},
 * and {@link Caption}. Connections are ref-counted: multiple callers opening the
 * same database path share one underlying {@link java.sql.Connection}.</p>
 *
 * <h2>Lifecycle</h2>
 * <pre>
 * PersistCrawlerIf db = PersistCrawlerIf.open("/path/to/db");
 * try {
 *     // ... CRUD calls ...
 * } finally {
 *     db.close();
 * }
 * </pre>
 *
 * <p>All tables are created on first {@code open()} if they do not already exist.
 * JSON serialisation uses the Jettison library
 * ({@link org.codehaus.jettison.json.JSONObject}).</p>
 */
public class PersistCrawlerIf {

    // ─── Static connection-management fields ─────────────────────────────────

    /** Shared connection pool keyed by database path. */
    private static final Map<String, Connection>    connectionMap = new ConcurrentHashMap<>();

    /** Reference counts for each database path in {@link #connectionMap}. */
    private static final Map<String, AtomicInteger> refCountMap   = new ConcurrentHashMap<>();

    /**
     * The connection most recently made active by {@link #open(String)}.
     * Read by cursor classes via {@link #getConnection()}.
     */
    private static volatile Connection activeConnection;

    /**
     * Counter used to generate unique Caption ids.
     * Initialised to {@code System.currentTimeMillis()} at class-load time and
     * incremented atomically by {@link #nextCaptionId()}. The id is only roughly
     * epoch-time; exact accuracy is not guaranteed.
     */
    private static final AtomicLong captionIdCounter =
            new AtomicLong(System.currentTimeMillis());

    // ─── Instance fields ──────────────────────────────────────────────────────

    /** The filesystem path to the database directory. */
    private final String     path;

    /** The JDBC connection used by this instance. */
    private final Connection conn;

    // Cached PreparedStatements — Poster

    /** Cached INSERT prepared statement for the Poster table. */
    private PreparedStatement psPosterInsert;

    /** Cached MERGE (upsert) prepared statement for the Poster table. */
    private PreparedStatement psPosterUpsert;

    /** Cached DELETE prepared statement for the Poster table. */
    private PreparedStatement psPosterDelete;

    /** Cached SELECT-by-PK prepared statement for the Poster table. */
    private PreparedStatement psPosterGet;

    // Cached PreparedStatements — Gallery

    /** Cached INSERT prepared statement for the Gallery table. */
    private PreparedStatement psGalleryInsert;

    /** Cached MERGE (upsert) prepared statement for the Gallery table. */
    private PreparedStatement psGalleryUpsert;

    /** Cached DELETE prepared statement for the Gallery table. */
    private PreparedStatement psGalleryDelete;

    /** Cached SELECT-by-PK prepared statement for the Gallery table. */
    private PreparedStatement psGalleryGet;

    // Cached PreparedStatements — Image

    /** Cached INSERT prepared statement for the Image table. */
    private PreparedStatement psImageInsert;

    /** Cached MERGE (upsert) prepared statement for the Image table. */
    private PreparedStatement psImageUpsert;

    /** Cached DELETE prepared statement for the Image table. */
    private PreparedStatement psImageDelete;

    /** Cached SELECT-by-PK prepared statement for the Image table. */
    private PreparedStatement psImageGet;

    // Cached PreparedStatements — GalleryAttr

    /** Cached INSERT prepared statement for the GalleryAttr table. */
    private PreparedStatement psGalleryAttrInsert;

    /** Cached MERGE (upsert) prepared statement for the GalleryAttr table. */
    private PreparedStatement psGalleryAttrUpsert;

    /** Cached DELETE prepared statement for the GalleryAttr table. */
    private PreparedStatement psGalleryAttrDelete;

    /** Cached SELECT-by-PK prepared statement for the GalleryAttr table. */
    private PreparedStatement psGalleryAttrGet;

    // Cached PreparedStatements — Caption

    /** Cached INSERT prepared statement for the Caption table. */
    private PreparedStatement psCaptionInsert;

    /** Cached MERGE (upsert) prepared statement for the Caption table. */
    private PreparedStatement psCaptionUpsert;

    /** Cached DELETE prepared statement for the Caption table. */
    private PreparedStatement psCaptionDelete;

    /** Cached SELECT-by-PK prepared statement for the Caption table. */
    private PreparedStatement psCaptionGet;

    // Client statement cache

    /** Reusable client-supplied prepared statements, keyed by SQL text. */
    private final Map<String, PreparedStatement> clientCache  = new ConcurrentHashMap<>();

    /**
     * Reverse map from a live client {@link PreparedStatement} to its SQL text,
     * used by {@link #recycle(PreparedStatement)} to return the statement to the cache.
     * Uses identity comparison so two distinct statements with the same SQL remain distinct.
     */
    private final Map<PreparedStatement, String> clientSqlMap =
            Collections.synchronizedMap(new IdentityHashMap<>());

    /**
     * Creates an instance bound to the given path and connection.
     * Use {@link #open(String)} to obtain instances.
     *
     * @param path the filesystem path to the database directory
     * @param conn the JDBC connection to use
     */
    private PersistCrawlerIf(String path, Connection conn) {
        this.path = path;
        this.conn = conn;
    }

    /**
     * Opens (or reuses) a database connection for the given path and returns a new instance.
     *
     * <p>If a connection to {@code dbPath} is already open it is reused and its
     * reference count is incremented. The connection is recorded as
     * {@link #activeConnection} so that cursor classes can retrieve it via
     * {@link #getConnection()}. All tables and indexes are created if they do not
     * already exist.</p>
     *
     * @param dbPath the filesystem directory that contains (or will contain) the database files
     * @return a new PersistCrawlerIf instance backed by the given path
     * @throws SQLException if the connection cannot be established or table creation fails
     */
    public static synchronized PersistCrawlerIf open(String dbPath) throws SQLException {
        Connection conn = connectionMap.get(dbPath);
        if (conn == null || conn.isClosed()) {
            conn = DriverManager.getConnection(
                    "jdbc:hsqldb:file:" + dbPath + "/db;shutdown=false", "SA", "");
            // Limit the in-memory row cache so HSQLDB cannot consume unbounded heap on large tables.
            // Without this, accessing a large Image table causes HSQLDB to cache all rows in RAM.
            try (Statement s = conn.createStatement()) {
                s.execute("SET FILES CACHE ROWS 10000");
                s.execute("SET FILES CACHE SIZE 65536");
                s.execute("SET DATABASE DEFAULT TABLE TYPE CACHED");
            }
            connectionMap.put(dbPath, conn);
            refCountMap.put(dbPath, new AtomicInteger(0));
        }
        refCountMap.get(dbPath).incrementAndGet();
        activeConnection = conn;
        PersistCrawlerIf p = new PersistCrawlerIf(dbPath, conn);
        p.createAllTablesAndIndexes();
        p.migrateTablesToCached();
        p.migrateImageOriginalFilename();
        return p;
    }

    /**
     * Returns the most recently activated HSQLDB connection.
     *
     * <p>Called by cursor classes (which live in a different package and cannot
     * access the private {@code conn} field directly).</p>
     *
     * @return the active connection; never {@code null}
     * @throws IllegalStateException if no connection has been opened yet
     */
    public static Connection getConnection() {
        Connection c = activeConnection;
        if (c == null) throw new IllegalStateException("No active connection");
        return c;
    }

    /**
     * Closes all cached prepared statements and releases the connection if no other
     * instances are still using it.
     *
     * <p>Decrements the reference count for this instance's database path. When the
     * count reaches zero the underlying {@link java.sql.Connection} is closed and
     * removed from the shared pool.</p>
     *
     * @throws SQLException if closing the connection fails
     */
    public synchronized void close() throws SQLException {
        psPosterInsert      = nullPs(psPosterInsert);
        psPosterUpsert      = nullPs(psPosterUpsert);
        psPosterDelete      = nullPs(psPosterDelete);
        psPosterGet         = nullPs(psPosterGet);
        psGalleryInsert     = nullPs(psGalleryInsert);
        psGalleryUpsert     = nullPs(psGalleryUpsert);
        psGalleryDelete     = nullPs(psGalleryDelete);
        psGalleryGet        = nullPs(psGalleryGet);
        psImageInsert       = nullPs(psImageInsert);
        psImageUpsert       = nullPs(psImageUpsert);
        psImageDelete       = nullPs(psImageDelete);
        psImageGet          = nullPs(psImageGet);
        psGalleryAttrInsert = nullPs(psGalleryAttrInsert);
        psGalleryAttrUpsert = nullPs(psGalleryAttrUpsert);
        psGalleryAttrDelete = nullPs(psGalleryAttrDelete);
        psGalleryAttrGet    = nullPs(psGalleryAttrGet);
        psCaptionInsert     = nullPs(psCaptionInsert);
        psCaptionUpsert     = nullPs(psCaptionUpsert);
        psCaptionDelete     = nullPs(psCaptionDelete);
        psCaptionGet        = nullPs(psCaptionGet);

        for (PreparedStatement ps : clientCache.values()) {
            try { ps.close(); } catch (Exception ignored) {}
        }
        clientCache.clear();
        clientSqlMap.clear();

        AtomicInteger ref = refCountMap.get(path);
        if (ref != null && ref.decrementAndGet() == 0) {
            connectionMap.remove(path);
            refCountMap.remove(path);
            conn.close();
            if (activeConnection == conn) activeConnection = null;
        }
    }

    /**
     * Closes and nulls a prepared statement, suppressing any exception.
     *
     * @param ps the statement to close; may be {@code null}
     * @return always {@code null}, enabling the idiom: {@code psField = nullPs(psField)}
     */
    private static PreparedStatement nullPs(PreparedStatement ps) {
        if (ps != null) { try { ps.close(); } catch (Exception ignored) {} }
        return null;
    }

    /**
     * Creates all tables and indexes on first open, skipping any that already exist.
     *
     * @throws SQLException if DDL execution fails
     */
    private void createAllTablesAndIndexes() throws SQLException {
        createPoster();
        createGallery();
        createImage();
        createGalleryAttr();
        createCaption();
    }

    /**
     * Converts any MEMORY tables to CACHED so they are disk-backed and subject to
     * the file cache limits set in {@link #open(String)}. This is a no-op for tables
     * that are already CACHED. Needed for databases created before this fix.
     */
    private void migrateTablesToCached() throws SQLException {
        String[] tables = {"Poster", "Gallery", "Image", "GalleryAttr", "Caption"};
        try (Statement s = conn.createStatement()) {
            for (String table : tables) {
                try {
                    s.execute("SET TABLE " + table + " TYPE CACHED");
                } catch (SQLException ignored) {
                    // Already CACHED, or table doesn't exist yet — either is fine
                }
            }
        }
    }

    /**
     * Adds the {@code originalFilename} column to the {@code Image} table if it does not
     * already exist, then populates it for any rows that have {@code imageUrl} set but
     * {@code originalFilename} still null.
     *
     * <p>This migration is idempotent: it is safe to call on every {@link #open(String)}
     * because the {@code ADD COLUMN} is silently skipped when the column is already present,
     * and the UPDATE only targets rows where the value is missing.</p>
     *
     * @throws SQLException if DDL or DML execution fails
     */
    private void migrateImageOriginalFilename() throws SQLException {
        // 1. Add column if not yet present
        try (Statement s = conn.createStatement()) {
            try {
                s.execute("ALTER TABLE Image ADD COLUMN originalFilename VARCHAR(32672)");
            } catch (SQLException ignored) {
                // Column already exists — that's fine
            }
        }
        // 2. Collect rows that need the value computed
        List<String[]> toUpdate = new ArrayList<>();
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT pageId, imageUrl FROM Image WHERE originalFilename IS NULL AND imageUrl IS NOT NULL")) {
            while (rs.next()) {
                toUpdate.add(new String[]{rs.getString("pageId"), rs.getString("imageUrl")});
            }
        }
        if (!toUpdate.isEmpty()) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE Image SET originalFilename = ? WHERE pageId = ?")) {
                for (String[] row : toUpdate) {
                    ps.setString(1, deriveOriginalFilename(row[1]));
                    ps.setString(2, row[0]);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }
    }

    /**
     * Derives the original filename from an image URL by stripping the query string and
     * returning the last path segment.
     *
     * @param imageUrl the image URL; must not be {@code null}
     * @return the filename portion of the URL
     */
    private static String deriveOriginalFilename(String imageUrl) {
        if (imageUrl.isBlank()) return imageUrl;
        String url = imageUrl;
        int q = url.indexOf('?');
        if (q >= 0) url = url.substring(0, q);
        int slash = url.lastIndexOf('/');
        return slash >= 0 ? url.substring(slash + 1) : url;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Poster
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Creates the {@code Poster} table and its indexes if they do not already exist.
     *
     * @throws SQLException if DDL execution fails
     */
    public void createPoster() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE CACHED TABLE IF NOT EXISTS Poster (" +
                      "name VARCHAR(32672) NOT NULL, " +
                      "url VARCHAR(32672), " +
                      "color VARCHAR(32672), " +
                      "PRIMARY KEY (name))");
            s.execute("CREATE INDEX IF NOT EXISTS idx_Poster_color ON Poster (color)");
        }
    }

    /**
     * Drops the {@code Poster} table and all its indexes.
     *
     * <p>All cached prepared statements for Poster are closed and nulled before the
     * drop to prevent HSQLDB from throwing because the statements still reference
     * the table.</p>
     *
     * @throws SQLException if DDL execution fails
     */
    public void dropPoster() throws SQLException {
        psPosterInsert = nullPs(psPosterInsert);
        psPosterUpsert = nullPs(psPosterUpsert);
        psPosterDelete = nullPs(psPosterDelete);
        psPosterGet    = nullPs(psPosterGet);
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS Poster");
        }
    }

    /**
     * Returns the cached INSERT prepared statement for Poster, (re)preparing if needed.
     *
     * @return the INSERT prepared statement
     * @throws SQLException if preparation fails
     */
    private PreparedStatement posterInsertStmt() throws SQLException {
        if (psPosterInsert == null || psPosterInsert.isClosed())
            psPosterInsert = conn.prepareStatement(
                    "INSERT INTO Poster (name, url, color) VALUES (?, ?, ?)");
        return psPosterInsert;
    }

    /**
     * Returns the cached MERGE (upsert) prepared statement for Poster, (re)preparing if needed.
     *
     * @return the MERGE prepared statement
     * @throws SQLException if preparation fails
     */
    private PreparedStatement posterUpsertStmt() throws SQLException {
        if (psPosterUpsert == null || psPosterUpsert.isClosed())
            psPosterUpsert = conn.prepareStatement(
                    "MERGE INTO Poster USING (VALUES (?, ?, ?)) AS t(name, url, color) " +
                    "ON Poster.name = t.name " +
                    "WHEN MATCHED THEN UPDATE SET url = t.url, color = t.color " +
                    "WHEN NOT MATCHED THEN INSERT (name, url, color) VALUES (t.name, t.url, t.color)");
        return psPosterUpsert;
    }

    /**
     * Returns the cached DELETE prepared statement for Poster, (re)preparing if needed.
     *
     * @return the DELETE prepared statement
     * @throws SQLException if preparation fails
     */
    private PreparedStatement posterDeleteStmt() throws SQLException {
        if (psPosterDelete == null || psPosterDelete.isClosed())
            psPosterDelete = conn.prepareStatement("DELETE FROM Poster WHERE name = ?");
        return psPosterDelete;
    }

    /**
     * Returns the cached SELECT-by-PK prepared statement for Poster, (re)preparing if needed.
     *
     * @return the SELECT-by-PK prepared statement
     * @throws SQLException if preparation fails
     */
    private PreparedStatement posterGetStmt() throws SQLException {
        if (psPosterGet == null || psPosterGet.isClosed())
            psPosterGet = conn.prepareStatement(
                    "SELECT name, url, color FROM Poster WHERE name = ?");
        return psPosterGet;
    }

    /**
     * Binds all fields of {@code r} to the given prepared statement in column order.
     *
     * @param ps the prepared statement to bind into
     * @param r  the Poster whose fields should be bound
     * @throws SQLException if a bind call fails
     */
    private static void bindPoster(PreparedStatement ps, Poster r) throws SQLException {
        ps.setString(1, r.getName());
        ps.setString(2, r.getUrl());
        ps.setString(3, r.getColor());
    }

    /**
     * Reads the current row of {@code rs} into a new {@link Poster}.
     *
     * @param rs a result set positioned on the row to read
     * @return a populated Poster
     * @throws SQLException if a column read fails
     */
    private static Poster posterFromRs(ResultSet rs) throws SQLException {
        Poster r = new Poster(rs.getString("name"));
        r.setUrl(rs.getString("url"));
        r.setColor(rs.getString("color"));
        return r;
    }

    /**
     * Opens a forward-only cursor over Poster rows matching the given SQL clauses.
     *
     * @param clauses optional SQL clauses to append (e.g. {@code "WHERE color = 'W'"}),
     *                or {@code null} to fetch all rows
     * @return a new {@link PosterCursor}
     * @throws SQLException if the query fails
     */
    public PosterCursor selectPoster(String clauses) throws SQLException {
        return PosterCursor.select(clauses);
    }

    /**
     * Inserts a single Poster row.
     *
     * @param row the Poster to insert
     * @return the row count (1 on success)
     * @throws SQLException if the insert fails (e.g. duplicate key)
     */
    public int insert(Poster row) throws SQLException {
        PreparedStatement ps = posterInsertStmt();
        bindPoster(ps, row);
        return ps.executeUpdate();
    }

    /**
     * Inserts all elements of {@code rows} as a batch.
     *
     * @param rows the rows to insert
     * @return per-row update counts from {@link PreparedStatement#executeBatch()}
     * @throws SQLException if the batch insert fails
     */
    public int[] insert(Poster[] rows) throws SQLException {
        return insert(rows, 0, rows.length);
    }

    /**
     * Inserts a slice of {@code rows} as a batch.
     *
     * @param rows   the source array
     * @param offset index of the first element to insert
     * @param len    number of elements to insert
     * @return per-row update counts from {@link PreparedStatement#executeBatch()}
     * @throws SQLException if the batch insert fails
     */
    public int[] insert(Poster[] rows, int offset, int len) throws SQLException {
        PreparedStatement ps = posterInsertStmt();
        for (int i = offset; i < offset + len; i++) { bindPoster(ps, rows[i]); ps.addBatch(); }
        return ps.executeBatch();
    }

    /**
     * Deletes the Poster row identified by {@code row}'s primary key.
     *
     * @param row the Poster to delete
     * @return the row count (1 if found, 0 if not)
     * @throws SQLException if the delete fails
     */
    public int delete(Poster row) throws SQLException {
        return deletePoster(row.getName());
    }

    /**
     * Deletes all elements of {@code rows} as a batch.
     *
     * @param rows the rows to delete
     * @return per-row update counts from {@link PreparedStatement#executeBatch()}
     * @throws SQLException if the batch delete fails
     */
    public int[] delete(Poster[] rows) throws SQLException {
        return delete(rows, 0, rows.length);
    }

    /**
     * Deletes a slice of {@code rows} as a batch.
     *
     * @param rows   the source array
     * @param offset index of the first element to delete
     * @param len    number of elements to delete
     * @return per-row update counts from {@link PreparedStatement#executeBatch()}
     * @throws SQLException if the batch delete fails
     */
    public int[] delete(Poster[] rows, int offset, int len) throws SQLException {
        PreparedStatement ps = posterDeleteStmt();
        for (int i = offset; i < offset + len; i++) { ps.setString(1, rows[i].getName()); ps.addBatch(); }
        return ps.executeBatch();
    }

    /**
     * Deletes the Poster row with the given primary key.
     *
     * @param name the poster name (primary key)
     * @return the row count (1 if found, 0 if not)
     * @throws SQLException if the delete fails
     */
    public int deletePoster(String name) throws SQLException {
        PreparedStatement ps = posterDeleteStmt();
        ps.setString(1, name);
        return ps.executeUpdate();
    }

    /**
     * Inserts or updates a single Poster row (MERGE / upsert).
     *
     * @param row the Poster to upsert
     * @return the row count
     * @throws SQLException if the upsert fails
     */
    public int upsert(Poster row) throws SQLException {
        PreparedStatement ps = posterUpsertStmt();
        bindPoster(ps, row);
        return ps.executeUpdate();
    }

    /**
     * Upserts all elements of {@code rows} as a batch.
     *
     * @param rows the rows to upsert
     * @return per-row update counts from {@link PreparedStatement#executeBatch()}
     * @throws SQLException if the batch upsert fails
     */
    public int[] upsert(Poster[] rows) throws SQLException {
        return upsert(rows, 0, rows.length);
    }

    /**
     * Upserts a slice of {@code rows} as a batch.
     *
     * @param rows   the source array
     * @param offset index of the first element to upsert
     * @param len    number of elements to upsert
     * @return per-row update counts from {@link PreparedStatement#executeBatch()}
     * @throws SQLException if the batch upsert fails
     */
    public int[] upsert(Poster[] rows, int offset, int len) throws SQLException {
        PreparedStatement ps = posterUpsertStmt();
        for (int i = offset; i < offset + len; i++) { bindPoster(ps, rows[i]); ps.addBatch(); }
        return ps.executeBatch();
    }

    /**
     * Retrieves the Poster with the given primary key.
     *
     * @param name the poster name (primary key)
     * @return the matching Poster, or {@code null} if not found
     * @throws SQLException if the query fails
     */
    public Poster getPoster(String name) throws SQLException {
        PreparedStatement ps = posterGetStmt();
        ps.setString(1, name);
        try (ResultSet rs = ps.executeQuery()) {
            return rs.next() ? posterFromRs(rs) : null;
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Gallery
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Creates the {@code Gallery} table and its indexes if they do not already exist.
     *
     * @throws SQLException if DDL execution fails
     */
    public void createGallery() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE CACHED TABLE IF NOT EXISTS Gallery (" +
                      "id VARCHAR(32672) NOT NULL, " +
                      "poster VARCHAR(32672), " +
                      "name VARCHAR(32672), " +
                      "url VARCHAR(32672), " +
                      "attrs VARCHAR(32672), " +
                      "images INTEGER, " +
                      "pages INTEGER, " +
                      "pagesSeen INTEGER, " +
                      "quality VARCHAR(32672), " +
                      "added VARCHAR(32672), " +
                      "state VARCHAR(32672), " +
                      "PRIMARY KEY (id))");
            s.execute("CREATE INDEX IF NOT EXISTS idx_Gallery_poster  ON Gallery (poster)");
            s.execute("CREATE INDEX IF NOT EXISTS idx_Gallery_quality ON Gallery (quality)");
            s.execute("CREATE INDEX IF NOT EXISTS idx_Gallery_added   ON Gallery (added)");
            s.execute("CREATE INDEX IF NOT EXISTS idx_Gallery_state   ON Gallery (state)");
        }
    }

    /**
     * Drops the {@code Gallery} table and all its indexes.
     *
     * <p>All cached prepared statements for Gallery are closed and nulled before the
     * drop to prevent HSQLDB from throwing because the statements still reference
     * the table.</p>
     *
     * @throws SQLException if DDL execution fails
     */
    public void dropGallery() throws SQLException {
        psGalleryInsert = nullPs(psGalleryInsert);
        psGalleryUpsert = nullPs(psGalleryUpsert);
        psGalleryDelete = nullPs(psGalleryDelete);
        psGalleryGet    = nullPs(psGalleryGet);
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS Gallery");
        }
    }

    /**
     * Returns the cached INSERT prepared statement for Gallery, (re)preparing if needed.
     *
     * @return the INSERT prepared statement
     * @throws SQLException if preparation fails
     */
    private PreparedStatement galleryInsertStmt() throws SQLException {
        if (psGalleryInsert == null || psGalleryInsert.isClosed())
            psGalleryInsert = conn.prepareStatement(
                    "INSERT INTO Gallery (id, poster, name, url, attrs, images, pages, pagesSeen, quality, added, state) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        return psGalleryInsert;
    }

    /**
     * Returns the cached MERGE (upsert) prepared statement for Gallery, (re)preparing if needed.
     *
     * @return the MERGE prepared statement
     * @throws SQLException if preparation fails
     */
    private PreparedStatement galleryUpsertStmt() throws SQLException {
        if (psGalleryUpsert == null || psGalleryUpsert.isClosed())
            psGalleryUpsert = conn.prepareStatement(
                    "MERGE INTO Gallery " +
                    "USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) " +
                    "AS t(id, poster, name, url, attrs, images, pages, pagesSeen, quality, added, state) " +
                    "ON Gallery.id = t.id " +
                    "WHEN MATCHED THEN UPDATE SET " +
                    "poster = t.poster, name = t.name, url = t.url, attrs = t.attrs, " +
                    "images = t.images, pages = t.pages, pagesSeen = t.pagesSeen, " +
                    "quality = t.quality, added = t.added, state = t.state " +
                    "WHEN NOT MATCHED THEN INSERT " +
                    "(id, poster, name, url, attrs, images, pages, pagesSeen, quality, added, state) " +
                    "VALUES (t.id, t.poster, t.name, t.url, t.attrs, t.images, t.pages, t.pagesSeen, t.quality, t.added, t.state)");
        return psGalleryUpsert;
    }

    /**
     * Returns the cached DELETE prepared statement for Gallery, (re)preparing if needed.
     *
     * @return the DELETE prepared statement
     * @throws SQLException if preparation fails
     */
    private PreparedStatement galleryDeleteStmt() throws SQLException {
        if (psGalleryDelete == null || psGalleryDelete.isClosed())
            psGalleryDelete = conn.prepareStatement("DELETE FROM Gallery WHERE id = ?");
        return psGalleryDelete;
    }

    /**
     * Returns the cached SELECT-by-PK prepared statement for Gallery, (re)preparing if needed.
     *
     * @return the SELECT-by-PK prepared statement
     * @throws SQLException if preparation fails
     */
    private PreparedStatement galleryGetStmt() throws SQLException {
        if (psGalleryGet == null || psGalleryGet.isClosed())
            psGalleryGet = conn.prepareStatement(
                    "SELECT id, poster, name, url, attrs, images, pages, pagesSeen, quality, added, state " +
                    "FROM Gallery WHERE id = ?");
        return psGalleryGet;
    }

    /**
     * Binds all fields of {@code r} to the given prepared statement in column order.
     * Integer fields use {@link #setIntOrNull} to correctly handle {@code null} values.
     *
     * @param ps the prepared statement to bind into
     * @param r  the Gallery whose fields should be bound
     * @throws SQLException if a bind call fails
     */
    private static void bindGallery(PreparedStatement ps, Gallery r) throws SQLException {
        ps.setString(1, r.getId());
        ps.setString(2, r.getPoster());
        ps.setString(3, r.getName());
        ps.setString(4, r.getUrl());
        ps.setString(5, r.getAttrs());
        setIntOrNull(ps, 6,  r.getImages());
        setIntOrNull(ps, 7,  r.getPages());
        setIntOrNull(ps, 8,  r.getPagesSeen());
        ps.setString(9,  r.getQuality());
        ps.setString(10, r.getAdded());
        ps.setString(11, r.getState());
    }

    /**
     * Reads the current row of {@code rs} into a new {@link Gallery}.
     * Integer columns ({@code images}, {@code pages}, {@code pagesSeen}) are read with
     * {@code wasNull()} to correctly handle SQL NULL.
     *
     * @param rs a result set positioned on the row to read
     * @return a populated Gallery
     * @throws SQLException if a column read fails
     */
    private static Gallery galleryFromRs(ResultSet rs) throws SQLException {
        Gallery r = new Gallery(rs.getString("id"));
        r.setPoster(rs.getString("poster"));
        r.setName(rs.getString("name"));
        r.setUrl(rs.getString("url"));
        r.setAttrs(rs.getString("attrs"));
        int images    = rs.getInt("images");    r.setImages(rs.wasNull()    ? null : images);
        int pages     = rs.getInt("pages");     r.setPages(rs.wasNull()     ? null : pages);
        int pagesSeen = rs.getInt("pagesSeen"); r.setPagesSeen(rs.wasNull() ? null : pagesSeen);
        r.setQuality(rs.getString("quality"));
        r.setAdded(rs.getString("added"));
        r.setState(rs.getString("state"));
        return r;
    }

    /**
     * Opens a forward-only cursor over Gallery rows matching the given SQL clauses.
     *
     * @param clauses optional SQL clauses to append (e.g. {@code "WHERE state = 'CA'"}),
     *                or {@code null} to fetch all rows
     * @return a new {@link GalleryCursor}
     * @throws SQLException if the query fails
     */
    public GalleryCursor selectGallery(String clauses) throws SQLException {
        return GalleryCursor.select(clauses);
    }

    /**
     * Inserts a single Gallery row.
     *
     * @param row the Gallery to insert
     * @return the row count (1 on success)
     * @throws SQLException if the insert fails (e.g. duplicate key)
     */
    public int insert(Gallery row) throws SQLException {
        PreparedStatement ps = galleryInsertStmt();
        bindGallery(ps, row);
        return ps.executeUpdate();
    }

    /**
     * Inserts all elements of {@code rows} as a batch.
     *
     * @param rows the rows to insert
     * @return per-row update counts from {@link PreparedStatement#executeBatch()}
     * @throws SQLException if the batch insert fails
     */
    public int[] insert(Gallery[] rows) throws SQLException {
        return insert(rows, 0, rows.length);
    }

    /**
     * Inserts a slice of {@code rows} as a batch.
     *
     * @param rows   the source array
     * @param offset index of the first element to insert
     * @param len    number of elements to insert
     * @return per-row update counts from {@link PreparedStatement#executeBatch()}
     * @throws SQLException if the batch insert fails
     */
    public int[] insert(Gallery[] rows, int offset, int len) throws SQLException {
        PreparedStatement ps = galleryInsertStmt();
        for (int i = offset; i < offset + len; i++) { bindGallery(ps, rows[i]); ps.addBatch(); }
        return ps.executeBatch();
    }

    /**
     * Deletes the Gallery row identified by {@code row}'s primary key.
     *
     * @param row the Gallery to delete
     * @return the row count (1 if found, 0 if not)
     * @throws SQLException if the delete fails
     */
    public int delete(Gallery row) throws SQLException {
        return deleteGallery(row.getId());
    }

    /**
     * Deletes all elements of {@code rows} as a batch.
     *
     * @param rows the rows to delete
     * @return per-row update counts from {@link PreparedStatement#executeBatch()}
     * @throws SQLException if the batch delete fails
     */
    public int[] delete(Gallery[] rows) throws SQLException {
        return delete(rows, 0, rows.length);
    }

    /**
     * Deletes a slice of {@code rows} as a batch.
     *
     * @param rows   the source array
     * @param offset index of the first element to delete
     * @param len    number of elements to delete
     * @return per-row update counts from {@link PreparedStatement#executeBatch()}
     * @throws SQLException if the batch delete fails
     */
    public int[] delete(Gallery[] rows, int offset, int len) throws SQLException {
        PreparedStatement ps = galleryDeleteStmt();
        for (int i = offset; i < offset + len; i++) { ps.setString(1, rows[i].getId()); ps.addBatch(); }
        return ps.executeBatch();
    }

    /**
     * Deletes the Gallery row with the given primary key.
     *
     * @param id the gallery id (primary key)
     * @return the row count (1 if found, 0 if not)
     * @throws SQLException if the delete fails
     */
    public int deleteGallery(String id) throws SQLException {
        PreparedStatement ps = galleryDeleteStmt();
        ps.setString(1, id);
        return ps.executeUpdate();
    }

    /**
     * Inserts or updates a single Gallery row (MERGE / upsert).
     *
     * @param row the Gallery to upsert
     * @return the row count
     * @throws SQLException if the upsert fails
     */
    public int upsert(Gallery row) throws SQLException {
        PreparedStatement ps = galleryUpsertStmt();
        bindGallery(ps, row);
        return ps.executeUpdate();
    }

    /**
     * Upserts all elements of {@code rows} as a batch.
     *
     * @param rows the rows to upsert
     * @return per-row update counts from {@link PreparedStatement#executeBatch()}
     * @throws SQLException if the batch upsert fails
     */
    public int[] upsert(Gallery[] rows) throws SQLException {
        return upsert(rows, 0, rows.length);
    }

    /**
     * Upserts a slice of {@code rows} as a batch.
     *
     * @param rows   the source array
     * @param offset index of the first element to upsert
     * @param len    number of elements to upsert
     * @return per-row update counts from {@link PreparedStatement#executeBatch()}
     * @throws SQLException if the batch upsert fails
     */
    public int[] upsert(Gallery[] rows, int offset, int len) throws SQLException {
        PreparedStatement ps = galleryUpsertStmt();
        for (int i = offset; i < offset + len; i++) { bindGallery(ps, rows[i]); ps.addBatch(); }
        return ps.executeBatch();
    }

    /**
     * Retrieves the Gallery with the given primary key.
     *
     * @param id the gallery id (primary key)
     * @return the matching Gallery, or {@code null} if not found
     * @throws SQLException if the query fails
     */
    public Gallery getGallery(String id) throws SQLException {
        PreparedStatement ps = galleryGetStmt();
        ps.setString(1, id);
        try (ResultSet rs = ps.executeQuery()) {
            return rs.next() ? galleryFromRs(rs) : null;
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Image
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Creates the {@code Image} table and its indexes if they do not already exist.
     *
     * @throws SQLException if DDL execution fails
     */
    public void createImage() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE CACHED TABLE IF NOT EXISTS Image (" +
                      "pageId VARCHAR(32672) NOT NULL, " +
                      "galleryId VARCHAR(32672), " +
                      "pageUrl VARCHAR(32672), " +
                      "imageId VARCHAR(32672), " +
                      "imageUrl VARCHAR(32672), " +
                      "filename VARCHAR(32672), " +
                      "score INTEGER, " +
                      "state VARCHAR(32672), " +
                      "originalFilename VARCHAR(32672), " +
                      "PRIMARY KEY (pageId))");
            s.execute("CREATE INDEX IF NOT EXISTS idx_Image_galleryId ON Image (galleryId)");
            s.execute("CREATE INDEX IF NOT EXISTS idx_Image_pageUrl   ON Image (pageUrl)");
            s.execute("CREATE INDEX IF NOT EXISTS idx_Image_imageUrl  ON Image (imageUrl)");
            s.execute("CREATE INDEX IF NOT EXISTS idx_Image_score     ON Image (score)");
            s.execute("CREATE INDEX IF NOT EXISTS idx_Image_state     ON Image (state)");
        }
    }

    /**
     * Drops the {@code Image} table and all its indexes.
     *
     * <p>All cached prepared statements for Image are closed and nulled before the
     * drop to prevent HSQLDB from throwing because the statements still reference
     * the table.</p>
     *
     * @throws SQLException if DDL execution fails
     */
    public void dropImage() throws SQLException {
        psImageInsert = nullPs(psImageInsert);
        psImageUpsert = nullPs(psImageUpsert);
        psImageDelete = nullPs(psImageDelete);
        psImageGet    = nullPs(psImageGet);
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS Image");
        }
    }

    /**
     * Returns the cached INSERT prepared statement for Image, (re)preparing if needed.
     *
     * @return the INSERT prepared statement
     * @throws SQLException if preparation fails
     */
    private PreparedStatement imageInsertStmt() throws SQLException {
        if (psImageInsert == null || psImageInsert.isClosed())
            psImageInsert = conn.prepareStatement(
                    "INSERT INTO Image (pageId, galleryId, pageUrl, imageId, imageUrl, filename, score, state, originalFilename) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
        return psImageInsert;
    }

    /**
     * Returns the cached MERGE (upsert) prepared statement for Image, (re)preparing if needed.
     *
     * @return the MERGE prepared statement
     * @throws SQLException if preparation fails
     */
    private PreparedStatement imageUpsertStmt() throws SQLException {
        if (psImageUpsert == null || psImageUpsert.isClosed())
            psImageUpsert = conn.prepareStatement(
                    "MERGE INTO Image " +
                    "USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)) " +
                    "AS t(pageId, galleryId, pageUrl, imageId, imageUrl, filename, score, state, originalFilename) " +
                    "ON Image.pageId = t.pageId " +
                    "WHEN MATCHED THEN UPDATE SET " +
                    "galleryId = t.galleryId, pageUrl = t.pageUrl, imageId = t.imageId, " +
                    "imageUrl = t.imageUrl, filename = t.filename, score = t.score, state = t.state, " +
                    "originalFilename = t.originalFilename " +
                    "WHEN NOT MATCHED THEN INSERT " +
                    "(pageId, galleryId, pageUrl, imageId, imageUrl, filename, score, state, originalFilename) " +
                    "VALUES (t.pageId, t.galleryId, t.pageUrl, t.imageId, t.imageUrl, t.filename, t.score, t.state, t.originalFilename)");
        return psImageUpsert;
    }

    /**
     * Returns the cached DELETE prepared statement for Image, (re)preparing if needed.
     *
     * @return the DELETE prepared statement
     * @throws SQLException if preparation fails
     */
    private PreparedStatement imageDeleteStmt() throws SQLException {
        if (psImageDelete == null || psImageDelete.isClosed())
            psImageDelete = conn.prepareStatement("DELETE FROM Image WHERE pageId = ?");
        return psImageDelete;
    }

    /**
     * Returns the cached SELECT-by-PK prepared statement for Image, (re)preparing if needed.
     *
     * @return the SELECT-by-PK prepared statement
     * @throws SQLException if preparation fails
     */
    private PreparedStatement imageGetStmt() throws SQLException {
        if (psImageGet == null || psImageGet.isClosed())
            psImageGet = conn.prepareStatement(
                    "SELECT pageId, galleryId, pageUrl, imageId, imageUrl, filename, score, state, originalFilename " +
                    "FROM Image WHERE pageId = ?");
        return psImageGet;
    }

    /**
     * Binds all fields of {@code r} to the given prepared statement in column order.
     * Integer fields use {@link #setIntOrNull} to correctly handle {@code null} values.
     *
     * @param ps the prepared statement to bind into
     * @param r  the Image whose fields should be bound
     * @throws SQLException if a bind call fails
     */
    private static void bindImage(PreparedStatement ps, Image r) throws SQLException {
        ps.setString(1, r.getPageId());
        ps.setString(2, r.getGalleryId());
        ps.setString(3, r.getPageUrl());
        ps.setString(4, r.getImageId());
        ps.setString(5, r.getImageUrl());
        ps.setString(6, r.getFilename());
        setIntOrNull(ps, 7, r.getScore());
        ps.setString(8, r.getState());
        ps.setString(9, r.getOriginalFilename());
    }

    /**
     * Reads the current row of {@code rs} into a new {@link Image}.
     * Integer columns ({@code score}) are read with {@code wasNull()} to correctly handle SQL NULL.
     *
     * @param rs a result set positioned on the row to read
     * @return a populated Image
     * @throws SQLException if a column read fails
     */
    private static Image imageFromRs(ResultSet rs) throws SQLException {
        Image r = new Image(rs.getString("pageId"));
        r.setGalleryId(rs.getString("galleryId"));
        r.setPageUrl(rs.getString("pageUrl"));
        r.setImageId(rs.getString("imageId"));
        r.setImageUrl(rs.getString("imageUrl"));
        r.setFilename(rs.getString("filename"));
        int score = rs.getInt("score"); r.setScore(rs.wasNull() ? null : score);
        r.setState(rs.getString("state"));
        r.setOriginalFilename(rs.getString("originalFilename"));
        return r;
    }

    /**
     * Opens a forward-only cursor over Image rows matching the given SQL clauses.
     *
     * @param clauses optional SQL clauses to append (e.g. {@code "WHERE state = 'DL'"}),
     *                or {@code null} to fetch all rows
     * @return a new {@link ImageCursor}
     * @throws SQLException if the query fails
     */
    public ImageCursor selectImage(String clauses) throws SQLException {
        return ImageCursor.select(clauses);
    }

    /**
     * Inserts a single Image row.
     *
     * @param row the Image to insert
     * @return the row count (1 on success)
     * @throws SQLException if the insert fails (e.g. duplicate key)
     */
    public int insert(Image row) throws SQLException {
        PreparedStatement ps = imageInsertStmt();
        bindImage(ps, row);
        return ps.executeUpdate();
    }

    /**
     * Inserts all elements of {@code rows} as a batch.
     *
     * @param rows the rows to insert
     * @return per-row update counts from {@link PreparedStatement#executeBatch()}
     * @throws SQLException if the batch insert fails
     */
    public int[] insert(Image[] rows) throws SQLException {
        return insert(rows, 0, rows.length);
    }

    /**
     * Inserts a slice of {@code rows} as a batch.
     *
     * @param rows   the source array
     * @param offset index of the first element to insert
     * @param len    number of elements to insert
     * @return per-row update counts from {@link PreparedStatement#executeBatch()}
     * @throws SQLException if the batch insert fails
     */
    public int[] insert(Image[] rows, int offset, int len) throws SQLException {
        PreparedStatement ps = imageInsertStmt();
        for (int i = offset; i < offset + len; i++) { bindImage(ps, rows[i]); ps.addBatch(); }
        return ps.executeBatch();
    }

    /**
     * Deletes the Image row identified by {@code row}'s primary key.
     *
     * @param row the Image to delete
     * @return the row count (1 if found, 0 if not)
     * @throws SQLException if the delete fails
     */
    public int delete(Image row) throws SQLException {
        return deleteImage(row.getPageId());
    }

    /**
     * Deletes all elements of {@code rows} as a batch.
     *
     * @param rows the rows to delete
     * @return per-row update counts from {@link PreparedStatement#executeBatch()}
     * @throws SQLException if the batch delete fails
     */
    public int[] delete(Image[] rows) throws SQLException {
        return delete(rows, 0, rows.length);
    }

    /**
     * Deletes a slice of {@code rows} as a batch.
     *
     * @param rows   the source array
     * @param offset index of the first element to delete
     * @param len    number of elements to delete
     * @return per-row update counts from {@link PreparedStatement#executeBatch()}
     * @throws SQLException if the batch delete fails
     */
    public int[] delete(Image[] rows, int offset, int len) throws SQLException {
        PreparedStatement ps = imageDeleteStmt();
        for (int i = offset; i < offset + len; i++) { ps.setString(1, rows[i].getPageId()); ps.addBatch(); }
        return ps.executeBatch();
    }

    /**
     * Deletes the Image row with the given primary key.
     *
     * @param pageId the image page id (primary key)
     * @return the row count (1 if found, 0 if not)
     * @throws SQLException if the delete fails
     */
    public int deleteImage(String pageId) throws SQLException {
        PreparedStatement ps = imageDeleteStmt();
        ps.setString(1, pageId);
        return ps.executeUpdate();
    }

    /**
     * Inserts or updates a single Image row (MERGE / upsert).
     *
     * @param row the Image to upsert
     * @return the row count
     * @throws SQLException if the upsert fails
     */
    public int upsert(Image row) throws SQLException {
        PreparedStatement ps = imageUpsertStmt();
        bindImage(ps, row);
        return ps.executeUpdate();
    }

    /**
     * Upserts all elements of {@code rows} as a batch.
     *
     * @param rows the rows to upsert
     * @return per-row update counts from {@link PreparedStatement#executeBatch()}
     * @throws SQLException if the batch upsert fails
     */
    public int[] upsert(Image[] rows) throws SQLException {
        return upsert(rows, 0, rows.length);
    }

    /**
     * Upserts a slice of {@code rows} as a batch.
     *
     * @param rows   the source array
     * @param offset index of the first element to upsert
     * @param len    number of elements to upsert
     * @return per-row update counts from {@link PreparedStatement#executeBatch()}
     * @throws SQLException if the batch upsert fails
     */
    public int[] upsert(Image[] rows, int offset, int len) throws SQLException {
        PreparedStatement ps = imageUpsertStmt();
        for (int i = offset; i < offset + len; i++) { bindImage(ps, rows[i]); ps.addBatch(); }
        return ps.executeBatch();
    }

    /**
     * Retrieves the Image with the given primary key.
     *
     * @param pageId the image page id (primary key)
     * @return the matching Image, or {@code null} if not found
     * @throws SQLException if the query fails
     */
    public Image getImage(String pageId) throws SQLException {
        PreparedStatement ps = imageGetStmt();
        ps.setString(1, pageId);
        try (ResultSet rs = ps.executeQuery()) {
            return rs.next() ? imageFromRs(rs) : null;
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // GalleryAttr
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Creates the {@code GalleryAttr} table and its indexes if they do not already exist.
     *
     * @throws SQLException if DDL execution fails
     */
    public void createGalleryAttr() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE CACHED TABLE IF NOT EXISTS GalleryAttr (" +
                      "name VARCHAR(32672) NOT NULL, " +
                      "score VARCHAR(32672), " +
                      "factor INTEGER, " +
                      "PRIMARY KEY (name))");
            s.execute("CREATE INDEX IF NOT EXISTS idx_GalleryAttr_score ON GalleryAttr (score)");
        }
    }

    /**
     * Drops the {@code GalleryAttr} table and all its indexes.
     *
     * <p>All cached prepared statements for GalleryAttr are closed and nulled before
     * the drop to prevent HSQLDB from throwing because the statements still reference
     * the table.</p>
     *
     * @throws SQLException if DDL execution fails
     */
    public void dropGalleryAttr() throws SQLException {
        psGalleryAttrInsert = nullPs(psGalleryAttrInsert);
        psGalleryAttrUpsert = nullPs(psGalleryAttrUpsert);
        psGalleryAttrDelete = nullPs(psGalleryAttrDelete);
        psGalleryAttrGet    = nullPs(psGalleryAttrGet);
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS GalleryAttr");
        }
    }

    /**
     * Returns the cached INSERT prepared statement for GalleryAttr, (re)preparing if needed.
     *
     * @return the INSERT prepared statement
     * @throws SQLException if preparation fails
     */
    private PreparedStatement galleryAttrInsertStmt() throws SQLException {
        if (psGalleryAttrInsert == null || psGalleryAttrInsert.isClosed())
            psGalleryAttrInsert = conn.prepareStatement(
                    "INSERT INTO GalleryAttr (name, score, factor) VALUES (?, ?, ?)");
        return psGalleryAttrInsert;
    }

    /**
     * Returns the cached MERGE (upsert) prepared statement for GalleryAttr, (re)preparing if needed.
     *
     * @return the MERGE prepared statement
     * @throws SQLException if preparation fails
     */
    private PreparedStatement galleryAttrUpsertStmt() throws SQLException {
        if (psGalleryAttrUpsert == null || psGalleryAttrUpsert.isClosed())
            psGalleryAttrUpsert = conn.prepareStatement(
                    "MERGE INTO GalleryAttr USING (VALUES (?, ?, ?)) AS t(name, score, factor) " +
                    "ON GalleryAttr.name = t.name " +
                    "WHEN MATCHED THEN UPDATE SET score = t.score, factor = t.factor " +
                    "WHEN NOT MATCHED THEN INSERT (name, score, factor) VALUES (t.name, t.score, t.factor)");
        return psGalleryAttrUpsert;
    }

    /**
     * Returns the cached DELETE prepared statement for GalleryAttr, (re)preparing if needed.
     *
     * @return the DELETE prepared statement
     * @throws SQLException if preparation fails
     */
    private PreparedStatement galleryAttrDeleteStmt() throws SQLException {
        if (psGalleryAttrDelete == null || psGalleryAttrDelete.isClosed())
            psGalleryAttrDelete = conn.prepareStatement("DELETE FROM GalleryAttr WHERE name = ?");
        return psGalleryAttrDelete;
    }

    /**
     * Returns the cached SELECT-by-PK prepared statement for GalleryAttr, (re)preparing if needed.
     *
     * @return the SELECT-by-PK prepared statement
     * @throws SQLException if preparation fails
     */
    private PreparedStatement galleryAttrGetStmt() throws SQLException {
        if (psGalleryAttrGet == null || psGalleryAttrGet.isClosed())
            psGalleryAttrGet = conn.prepareStatement(
                    "SELECT name, score, factor FROM GalleryAttr WHERE name = ?");
        return psGalleryAttrGet;
    }

    /**
     * Binds all fields of {@code r} to the given prepared statement in column order.
     * The Integer field {@code factor} uses {@link #setIntOrNull} to correctly handle
     * {@code null} values.
     *
     * @param ps the prepared statement to bind into
     * @param r  the GalleryAttr whose fields should be bound
     * @throws SQLException if a bind call fails
     */
    private static void bindGalleryAttr(PreparedStatement ps, GalleryAttr r) throws SQLException {
        ps.setString(1, r.getName());
        ps.setString(2, r.getScore());
        setIntOrNull(ps, 3, r.getFactor());
    }

    /**
     * Reads the current row of {@code rs} into a new {@link GalleryAttr}.
     * The Integer column {@code factor} is read with {@code wasNull()} to distinguish
     * SQL NULL from zero.
     *
     * @param rs a result set positioned on the row to read
     * @return a populated GalleryAttr
     * @throws SQLException if a column read fails
     */
    private static GalleryAttr galleryAttrFromRs(ResultSet rs) throws SQLException {
        GalleryAttr r = new GalleryAttr(rs.getString("name"));
        r.setScore(rs.getString("score"));
        int factor = rs.getInt("factor"); r.setFactor(rs.wasNull() ? null : factor);
        return r;
    }

    /**
     * Opens a forward-only cursor over GalleryAttr rows matching the given SQL clauses.
     *
     * @param clauses optional SQL clauses to append (e.g. {@code "WHERE score = '+'"}),
     *                or {@code null} to fetch all rows
     * @return a new {@link GalleryAttrCursor}
     * @throws SQLException if the query fails
     */
    public GalleryAttrCursor selectGalleryAttr(String clauses) throws SQLException {
        return GalleryAttrCursor.select(clauses);
    }

    /**
     * Inserts a single GalleryAttr row.
     *
     * @param row the GalleryAttr to insert
     * @return the row count (1 on success)
     * @throws SQLException if the insert fails (e.g. duplicate key)
     */
    public int insert(GalleryAttr row) throws SQLException {
        PreparedStatement ps = galleryAttrInsertStmt();
        bindGalleryAttr(ps, row);
        return ps.executeUpdate();
    }

    /**
     * Inserts all elements of {@code rows} as a batch.
     *
     * @param rows the rows to insert
     * @return per-row update counts from {@link PreparedStatement#executeBatch()}
     * @throws SQLException if the batch insert fails
     */
    public int[] insert(GalleryAttr[] rows) throws SQLException {
        return insert(rows, 0, rows.length);
    }

    /**
     * Inserts a slice of {@code rows} as a batch.
     *
     * @param rows   the source array
     * @param offset index of the first element to insert
     * @param len    number of elements to insert
     * @return per-row update counts from {@link PreparedStatement#executeBatch()}
     * @throws SQLException if the batch insert fails
     */
    public int[] insert(GalleryAttr[] rows, int offset, int len) throws SQLException {
        PreparedStatement ps = galleryAttrInsertStmt();
        for (int i = offset; i < offset + len; i++) { bindGalleryAttr(ps, rows[i]); ps.addBatch(); }
        return ps.executeBatch();
    }

    /**
     * Deletes the GalleryAttr row identified by {@code row}'s primary key.
     *
     * @param row the GalleryAttr to delete
     * @return the row count (1 if found, 0 if not)
     * @throws SQLException if the delete fails
     */
    public int delete(GalleryAttr row) throws SQLException {
        return deleteGalleryAttr(row.getName());
    }

    /**
     * Deletes all elements of {@code rows} as a batch.
     *
     * @param rows the rows to delete
     * @return per-row update counts from {@link PreparedStatement#executeBatch()}
     * @throws SQLException if the batch delete fails
     */
    public int[] delete(GalleryAttr[] rows) throws SQLException {
        return delete(rows, 0, rows.length);
    }

    /**
     * Deletes a slice of {@code rows} as a batch.
     *
     * @param rows   the source array
     * @param offset index of the first element to delete
     * @param len    number of elements to delete
     * @return per-row update counts from {@link PreparedStatement#executeBatch()}
     * @throws SQLException if the batch delete fails
     */
    public int[] delete(GalleryAttr[] rows, int offset, int len) throws SQLException {
        PreparedStatement ps = galleryAttrDeleteStmt();
        for (int i = offset; i < offset + len; i++) { ps.setString(1, rows[i].getName()); ps.addBatch(); }
        return ps.executeBatch();
    }

    /**
     * Deletes the GalleryAttr row with the given primary key.
     *
     * @param name the attribute name (primary key)
     * @return the row count (1 if found, 0 if not)
     * @throws SQLException if the delete fails
     */
    public int deleteGalleryAttr(String name) throws SQLException {
        PreparedStatement ps = galleryAttrDeleteStmt();
        ps.setString(1, name);
        return ps.executeUpdate();
    }

    /**
     * Inserts or updates a single GalleryAttr row (MERGE / upsert).
     *
     * @param row the GalleryAttr to upsert
     * @return the row count
     * @throws SQLException if the upsert fails
     */
    public int upsert(GalleryAttr row) throws SQLException {
        PreparedStatement ps = galleryAttrUpsertStmt();
        bindGalleryAttr(ps, row);
        return ps.executeUpdate();
    }

    /**
     * Upserts all elements of {@code rows} as a batch.
     *
     * @param rows the rows to upsert
     * @return per-row update counts from {@link PreparedStatement#executeBatch()}
     * @throws SQLException if the batch upsert fails
     */
    public int[] upsert(GalleryAttr[] rows) throws SQLException {
        return upsert(rows, 0, rows.length);
    }

    /**
     * Upserts a slice of {@code rows} as a batch.
     *
     * @param rows   the source array
     * @param offset index of the first element to upsert
     * @param len    number of elements to upsert
     * @return per-row update counts from {@link PreparedStatement#executeBatch()}
     * @throws SQLException if the batch upsert fails
     */
    public int[] upsert(GalleryAttr[] rows, int offset, int len) throws SQLException {
        PreparedStatement ps = galleryAttrUpsertStmt();
        for (int i = offset; i < offset + len; i++) { bindGalleryAttr(ps, rows[i]); ps.addBatch(); }
        return ps.executeBatch();
    }

    /**
     * Retrieves the GalleryAttr with the given primary key.
     *
     * @param name the attribute name (primary key)
     * @return the matching GalleryAttr, or {@code null} if not found
     * @throws SQLException if the query fails
     */
    public GalleryAttr getGalleryAttr(String name) throws SQLException {
        PreparedStatement ps = galleryAttrGetStmt();
        ps.setString(1, name);
        try (ResultSet rs = ps.executeQuery()) {
            return rs.next() ? galleryAttrFromRs(rs) : null;
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Caption
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Returns the next unique Caption id.
     *
     * <p>The counter is initialised to {@code System.currentTimeMillis()} at class-load
     * time and incremented atomically on each call. The returned value is only
     * approximately the current time; exact accuracy is not guaranteed.</p>
     *
     * @return a unique {@code long} suitable for use as a Caption primary key
     */
    public static long nextCaptionId() {
        return captionIdCounter.getAndIncrement();
    }

    /**
     * Creates the {@code Caption} table if it does not already exist.
     *
     * @throws SQLException if DDL execution fails
     */
    public void createCaption() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE CACHED TABLE IF NOT EXISTS Caption (" +
                      "id BIGINT NOT NULL, " +
                      "imageId VARCHAR(32672), " +
                      "caption VARCHAR(32672), " +
                      "PRIMARY KEY (id))");
        }
    }

    /**
     * Drops the {@code Caption} table.
     *
     * <p>All cached prepared statements for Caption are closed and nulled before the
     * drop to prevent HSQLDB from throwing because the statements still reference
     * the table.</p>
     *
     * @throws SQLException if DDL execution fails
     */
    public void dropCaption() throws SQLException {
        psCaptionInsert = nullPs(psCaptionInsert);
        psCaptionUpsert = nullPs(psCaptionUpsert);
        psCaptionDelete = nullPs(psCaptionDelete);
        psCaptionGet    = nullPs(psCaptionGet);
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS Caption");
        }
    }

    /**
     * Returns the cached INSERT prepared statement for Caption, (re)preparing if needed.
     *
     * @return the INSERT prepared statement
     * @throws SQLException if preparation fails
     */
    private PreparedStatement captionInsertStmt() throws SQLException {
        if (psCaptionInsert == null || psCaptionInsert.isClosed())
            psCaptionInsert = conn.prepareStatement(
                    "INSERT INTO Caption (id, imageId, caption) VALUES (?, ?, ?)");
        return psCaptionInsert;
    }

    /**
     * Returns the cached MERGE (upsert) prepared statement for Caption, (re)preparing if needed.
     *
     * @return the MERGE prepared statement
     * @throws SQLException if preparation fails
     */
    private PreparedStatement captionUpsertStmt() throws SQLException {
        if (psCaptionUpsert == null || psCaptionUpsert.isClosed())
            psCaptionUpsert = conn.prepareStatement(
                    "MERGE INTO Caption USING (VALUES (?, ?, ?)) AS t(id, imageId, caption) " +
                    "ON Caption.id = t.id " +
                    "WHEN MATCHED THEN UPDATE SET imageId = t.imageId, caption = t.caption " +
                    "WHEN NOT MATCHED THEN INSERT (id, imageId, caption) VALUES (t.id, t.imageId, t.caption)");
        return psCaptionUpsert;
    }

    /**
     * Returns the cached DELETE prepared statement for Caption, (re)preparing if needed.
     *
     * @return the DELETE prepared statement
     * @throws SQLException if preparation fails
     */
    private PreparedStatement captionDeleteStmt() throws SQLException {
        if (psCaptionDelete == null || psCaptionDelete.isClosed())
            psCaptionDelete = conn.prepareStatement("DELETE FROM Caption WHERE id = ?");
        return psCaptionDelete;
    }

    /**
     * Returns the cached SELECT-by-PK prepared statement for Caption, (re)preparing if needed.
     *
     * @return the SELECT-by-PK prepared statement
     * @throws SQLException if preparation fails
     */
    private PreparedStatement captionGetStmt() throws SQLException {
        if (psCaptionGet == null || psCaptionGet.isClosed())
            psCaptionGet = conn.prepareStatement(
                    "SELECT id, imageId, caption FROM Caption WHERE id = ?");
        return psCaptionGet;
    }

    /**
     * Binds all fields of {@code r} to the given prepared statement in column order.
     *
     * @param ps the prepared statement to bind into
     * @param r  the Caption whose fields should be bound
     * @throws SQLException if a bind call fails
     */
    private static void bindCaption(PreparedStatement ps, Caption r) throws SQLException {
        ps.setLong(1, r.getId());
        ps.setString(2, r.getImageId());
        ps.setString(3, r.getCaption());
    }

    /**
     * Reads the current row of {@code rs} into a new {@link Caption}.
     *
     * @param rs a result set positioned on the row to read
     * @return a populated Caption
     * @throws SQLException if a column read fails
     */
    private static Caption captionFromRs(ResultSet rs) throws SQLException {
        Caption r = new Caption(rs.getLong("id"));
        r.setImageId(rs.getString("imageId"));
        r.setCaption(rs.getString("caption"));
        return r;
    }

    /**
     * Opens a forward-only cursor over Caption rows matching the given SQL clauses.
     *
     * @param clauses optional SQL clauses to append (e.g. {@code "WHERE imageId = '123'"}),
     *                or {@code null} to fetch all rows
     * @return a new {@link CaptionCursor}
     * @throws SQLException if the query fails
     */
    public CaptionCursor selectCaption(String clauses) throws SQLException {
        return CaptionCursor.select(clauses);
    }

    /**
     * Inserts a single Caption row.
     *
     * @param row the Caption to insert
     * @return the row count (1 on success)
     * @throws SQLException if the insert fails (e.g. duplicate key)
     */
    public int insert(Caption row) throws SQLException {
        PreparedStatement ps = captionInsertStmt();
        bindCaption(ps, row);
        return ps.executeUpdate();
    }

    /**
     * Inserts all elements of {@code rows} as a batch.
     *
     * @param rows the rows to insert
     * @return per-row update counts from {@link PreparedStatement#executeBatch()}
     * @throws SQLException if the batch insert fails
     */
    public int[] insert(Caption[] rows) throws SQLException {
        return insert(rows, 0, rows.length);
    }

    /**
     * Inserts a slice of {@code rows} as a batch.
     *
     * @param rows   the source array
     * @param offset index of the first element to insert
     * @param len    number of elements to insert
     * @return per-row update counts from {@link PreparedStatement#executeBatch()}
     * @throws SQLException if the batch insert fails
     */
    public int[] insert(Caption[] rows, int offset, int len) throws SQLException {
        PreparedStatement ps = captionInsertStmt();
        for (int i = offset; i < offset + len; i++) { bindCaption(ps, rows[i]); ps.addBatch(); }
        return ps.executeBatch();
    }

    /**
     * Deletes the Caption row identified by {@code row}'s primary key.
     *
     * @param row the Caption to delete
     * @return the row count (1 if found, 0 if not)
     * @throws SQLException if the delete fails
     */
    public int delete(Caption row) throws SQLException {
        return deleteCaption(row.getId());
    }

    /**
     * Deletes all elements of {@code rows} as a batch.
     *
     * @param rows the rows to delete
     * @return per-row update counts from {@link PreparedStatement#executeBatch()}
     * @throws SQLException if the batch delete fails
     */
    public int[] delete(Caption[] rows) throws SQLException {
        return delete(rows, 0, rows.length);
    }

    /**
     * Deletes a slice of {@code rows} as a batch.
     *
     * @param rows   the source array
     * @param offset index of the first element to delete
     * @param len    number of elements to delete
     * @return per-row update counts from {@link PreparedStatement#executeBatch()}
     * @throws SQLException if the batch delete fails
     */
    public int[] delete(Caption[] rows, int offset, int len) throws SQLException {
        PreparedStatement ps = captionDeleteStmt();
        for (int i = offset; i < offset + len; i++) { ps.setLong(1, rows[i].getId()); ps.addBatch(); }
        return ps.executeBatch();
    }

    /**
     * Deletes the Caption row with the given primary key.
     *
     * @param id the caption id (primary key)
     * @return the row count (1 if found, 0 if not)
     * @throws SQLException if the delete fails
     */
    public int deleteCaption(long id) throws SQLException {
        PreparedStatement ps = captionDeleteStmt();
        ps.setLong(1, id);
        return ps.executeUpdate();
    }

    /**
     * Inserts or updates a single Caption row (MERGE / upsert).
     *
     * @param row the Caption to upsert
     * @return the row count
     * @throws SQLException if the upsert fails
     */
    public int upsert(Caption row) throws SQLException {
        PreparedStatement ps = captionUpsertStmt();
        bindCaption(ps, row);
        return ps.executeUpdate();
    }

    /**
     * Upserts all elements of {@code rows} as a batch.
     *
     * @param rows the rows to upsert
     * @return per-row update counts from {@link PreparedStatement#executeBatch()}
     * @throws SQLException if the batch upsert fails
     */
    public int[] upsert(Caption[] rows) throws SQLException {
        return upsert(rows, 0, rows.length);
    }

    /**
     * Upserts a slice of {@code rows} as a batch.
     *
     * @param rows   the source array
     * @param offset index of the first element to upsert
     * @param len    number of elements to upsert
     * @return per-row update counts from {@link PreparedStatement#executeBatch()}
     * @throws SQLException if the batch upsert fails
     */
    public int[] upsert(Caption[] rows, int offset, int len) throws SQLException {
        PreparedStatement ps = captionUpsertStmt();
        for (int i = offset; i < offset + len; i++) { bindCaption(ps, rows[i]); ps.addBatch(); }
        return ps.executeBatch();
    }

    /**
     * Retrieves the Caption with the given primary key.
     *
     * @param id the caption id (primary key)
     * @return the matching Caption, or {@code null} if not found
     * @throws SQLException if the query fails
     */
    public Caption getCaption(long id) throws SQLException {
        PreparedStatement ps = captionGetStmt();
        ps.setLong(1, id);
        try (ResultSet rs = ps.executeQuery()) {
            return rs.next() ? captionFromRs(rs) : null;
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Null-safe bind helpers
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Binds an {@code Integer} parameter, using SQL NULL when the value is {@code null}.
     *
     * @param ps  the prepared statement
     * @param idx the 1-based parameter index
     * @param v   the value to bind, or {@code null}
     * @throws SQLException if the bind call fails
     */
    private static void setIntOrNull(PreparedStatement ps, int idx, Integer v) throws SQLException {
        if (v == null) ps.setNull(idx, Types.INTEGER); else ps.setInt(idx, v);
    }

    /**
     * Binds a {@code Boolean} parameter, using SQL NULL when the value is {@code null}.
     *
     * @param ps  the prepared statement
     * @param idx the 1-based parameter index
     * @param v   the value to bind, or {@code null}
     * @throws SQLException if the bind call fails
     */
    private static void setBoolOrNull(PreparedStatement ps, int idx, Boolean v) throws SQLException {
        if (v == null) ps.setNull(idx, Types.BOOLEAN); else ps.setBoolean(idx, v);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Client PreparedStatement helpers
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Returns a prepared statement for the given SQL, reusing a cached instance if available.
     *
     * <p>Uses {@code clientCache.remove(sql)} (not {@code get}) to atomically take the
     * entry from the cache, avoiding concurrent double-use of the same statement.
     * The returned statement is registered in {@link #clientSqlMap} so that
     * {@link #recycle(PreparedStatement)} can return it to the cache.</p>
     *
     * @param sql the SQL text to prepare
     * @return a prepared statement for {@code sql}
     * @throws SQLException if preparation fails
     */
    public PreparedStatement prepare(String sql) throws SQLException {
        PreparedStatement ps = clientCache.remove(sql);
        if (ps != null && !ps.isClosed()) {
            clientSqlMap.put(ps, sql);
            return ps;
        }
        ps = conn.prepareStatement(sql);
        clientSqlMap.put(ps, sql);
        return ps;
    }

    /**
     * Executes the given prepared statement.
     *
     * @param ps the statement to execute (parameters must be bound before calling)
     * @throws SQLException if execution fails
     */
    public void execute(PreparedStatement ps) throws SQLException {
        ps.execute();
    }

    /**
     * Returns a prepared statement to the cache so it can be reused by a future
     * {@link #prepare(String)} call.
     *
     * <p>Clears the statement's parameters before caching. If the statement is
     * already closed or its SQL is not tracked, it is silently discarded.</p>
     *
     * @param ps the statement to recycle
     */
    public void recycle(PreparedStatement ps) {
        String sql = clientSqlMap.remove(ps);
        if (sql != null && !isPsClosed(ps)) {
            try { ps.clearParameters(); } catch (SQLException ignored) {}
            clientCache.put(sql, ps);
        }
    }

    /**
     * Executes a scalar query and returns the first column of the first row as a {@code long}.
     *
     * <p>Suitable for aggregate queries such as {@code SELECT COUNT(*) FROM ...} or
     * {@code SELECT MAX(id) FROM ...}.</p>
     *
     * @param sql the SQL query to execute
     * @return the first column of the first row as a {@code long}, or {@code 0L} if no rows
     * @throws SQLException if the query fails
     */
    public long getValue(String sql) throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getLong(1) : 0L;
        }
    }

    /**
     * Returns {@code true} if the given prepared statement is closed (or throws an exception
     * when queried), {@code false} otherwise.
     *
     * @param ps the statement to check
     * @return {@code true} if closed or unusable
     */
    private static boolean isPsClosed(PreparedStatement ps) {
        try { return ps.isClosed(); } catch (SQLException e) { return true; }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Backup
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Backs up the database to a directory of newline-delimited JSON files.
     *
     * <p>Creates one {@code TypeName.json} file per model type in {@code backupPath}.
     * Each line is a single JSON object produced by the bean's {@code asJson()} method.
     * The four types are written in parallel — one thread each — using dedicated
     * connections so threads do not share a {@link java.sql.Connection}.</p>
     *
     * @param backupPath the directory to write backup files into (created if absent)
     * @throws Exception if any thread fails or an I/O / SQL error occurs
     */
    public void backup(String backupPath) throws Exception {
        Files.createDirectories(Paths.get(backupPath));
        logHeap("backup start");
        ExecutorService executor = Executors.newFixedThreadPool(5);
        List<Future<?>> futures = new ArrayList<>();

        futures.add(executor.submit((Callable<Void>) () -> {
            try (Connection c = newConn();
                 Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT name, url, color FROM Poster")) {
                try (BufferedWriter w = Files.newBufferedWriter(Paths.get(backupPath, "Poster.json"))) {
                    while (rs.next()) { w.write(posterFromRs(rs).asJson().toString()); w.newLine(); }
                }
            }
            return null;
        }));

        futures.add(executor.submit((Callable<Void>) () -> {
            try (Connection c = newConn();
                 Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT id, poster, name, url, attrs, images, pages, pagesSeen, quality, added, state FROM Gallery")) {
                try (BufferedWriter w = Files.newBufferedWriter(Paths.get(backupPath, "Gallery.json"))) {
                    while (rs.next()) { w.write(galleryFromRs(rs).asJson().toString()); w.newLine(); }
                }
            }
            return null;
        }));

        futures.add(executor.submit((Callable<Void>) () -> {
            try (Connection c = newConn();
                 Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT pageId, galleryId, pageUrl, imageId, imageUrl, filename, score, state, originalFilename FROM Image")) {
                try (BufferedWriter w = Files.newBufferedWriter(Paths.get(backupPath, "Image.json"))) {
                    while (rs.next()) { w.write(imageFromRs(rs).asJson().toString()); w.newLine(); }
                }
            }
            return null;
        }));

        futures.add(executor.submit((Callable<Void>) () -> {
            try (Connection c = newConn();
                 Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT name, score, factor FROM GalleryAttr")) {
                try (BufferedWriter w = Files.newBufferedWriter(Paths.get(backupPath, "GalleryAttr.json"))) {
                    while (rs.next()) { w.write(galleryAttrFromRs(rs).asJson().toString()); w.newLine(); }
                }
            }
            return null;
        }));

        futures.add(executor.submit((Callable<Void>) () -> {
            try (Connection c = newConn();
                 Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT id, imageId, caption FROM Caption")) {
                try (BufferedWriter w = Files.newBufferedWriter(Paths.get(backupPath, "Caption.json"))) {
                    while (rs.next()) { w.write(captionFromRs(rs).asJson().toString()); w.newLine(); }
                }
            }
            return null;
        }));

        executor.shutdown();
        for (Future<?> f : futures) f.get();
        logHeap("backup end");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Restore
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Restores the database from a directory of newline-delimited JSON files.
     *
     * <p>Drops and recreates all tables before loading. Reads each {@code TypeName.json}
     * file in {@code backupPath} and upserts the parsed beans in parallel — one thread
     * per type — on the shared connection. Missing backup files are silently ignored.</p>
     *
     * @param backupPath the directory containing the backup JSON files
     * @throws Exception if any thread fails or an I/O / SQL error occurs
     */
    public void restore(String backupPath) throws Exception {
        dropPoster();      createPoster();
        dropGallery();     createGallery();
        dropImage();       createImage();
        dropGalleryAttr(); createGalleryAttr();
        dropCaption();     createCaption();

        restoreTable(backupPath, "Poster",      line -> upsert(new Poster(new JSONObject(line))));
        restoreTable(backupPath, "Gallery",     line -> upsert(new Gallery(new JSONObject(line))));
        restoreTable(backupPath, "Image",       line -> upsert(new Image(new JSONObject(line))));
        restoreTable(backupPath, "GalleryAttr", line -> upsert(new GalleryAttr(new JSONObject(line))));
        restoreTable(backupPath, "Caption",     line -> upsert(new Caption(new JSONObject(line))));
    }

    @FunctionalInterface
    private interface LineProcessor {
        void process(String line) throws Exception;
    }

    private static void restoreTable(String backupPath, String typeName, LineProcessor processor) throws Exception {
        Path file = Paths.get(backupPath, typeName + ".json");
        if (!Files.exists(file)) return;
        logHeap("restore " + typeName + " start");
        try (BufferedReader r = Files.newBufferedReader(file)) {
            String line;
            while ((line = r.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty()) processor.process(line);
            }
        }
        logHeap("restore " + typeName + " end");
    }

    private static void logHeap(String label) {
        Runtime rt = Runtime.getRuntime();
        long usedMb = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
        System.out.println("[heap] " + label + " used=" + usedMb + "MB total=" + rt.totalMemory() / (1024 * 1024) + "MB max=" + rt.maxMemory() / (1024 * 1024) + "MB");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Internal helpers
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Opens a new, independent JDBC connection to this instance's database path.
     *
     * <p>Used by backup threads so each thread has its own connection and does not
     * interfere with the shared instance connection.</p>
     *
     * @return a new connection to the same database
     * @throws SQLException if the connection cannot be established
     */
    private Connection newConn() throws SQLException {
        return DriverManager.getConnection(
                "jdbc:hsqldb:file:" + path + "/db;shutdown=false", "SA", "");
    }

    /**
     * Reads all non-blank lines from {@code typeName.json} in {@code backupPath} and
     * parses each line as a {@link JSONObject}.
     *
     * <p>Returns an empty list if the file does not exist.</p>
     *
     * @param backupPath the directory containing the backup file
     * @param typeName   the model type name (used to form the filename, e.g. {@code "Poster"})
     * @return an ordered list of parsed JSON objects, one per non-blank line
     * @throws Exception if the file cannot be read or a line is not valid JSON
     */
    private static List<JSONObject> readJsonLines(String backupPath, String typeName) throws Exception {
        Path file = Paths.get(backupPath, typeName + ".json");
        if (!Files.exists(file)) return Collections.emptyList();
        List<JSONObject> result = new ArrayList<>();
        try (BufferedReader r = Files.newBufferedReader(file)) {
            String line;
            while ((line = r.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty()) result.add(new JSONObject(line));
            }
        }
        return result;
    }
}
