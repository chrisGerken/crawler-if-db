package com.gerken.db.cursor;

import com.gerken.db.bean.GalleryAttr;
import com.gerken.db.persist.PersistCrawlerIf;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Forward-only, read-only cursor over a {@link GalleryAttr} result set.
 *
 * <p>Obtain a cursor via {@link #select(String)}, iterate with {@link #next()},
 * and close via {@link #close()} (called automatically when iteration is exhausted).
 * Use {@link #getAll()} to materialise the full result set into an array, or
 * {@link #count(String)} to count matching rows without fetching them.</p>
 *
 * <p>The cursor borrows the active HSQLDB connection from
 * {@link PersistCrawlerIf#getConnection()}; it does not own a connection.</p>
 */
public class GalleryAttrCursor {

    /** The prepared statement whose result set this cursor iterates. */
    private final PreparedStatement ps;

    /** The live result set being iterated. */
    private final ResultSet rs;

    /**
     * Creates a GalleryAttrCursor wrapping the given statement and result set.
     *
     * @param ps the prepared statement that produced {@code rs}
     * @param rs the result set to iterate
     */
    private GalleryAttrCursor(PreparedStatement ps, ResultSet rs) {
        this.ps = ps;
        this.rs = rs;
    }

    /**
     * Opens a cursor over all GalleryAttr rows that match the given SQL clauses.
     *
     * <p>The cursor reads all columns ({@code name, good, bad}) from the
     * {@code GalleryAttr} table. The optional {@code clauses} string is appended
     * directly to the base SELECT; it may contain {@code WHERE}, {@code ORDER BY},
     * {@code LIMIT}, or any other trailing SQL fragment.</p>
     *
     * @param clauses optional SQL clauses to append (e.g. {@code "WHERE good = TRUE"}),
     *                or {@code null} / blank to fetch all rows
     * @return an open cursor positioned before the first row
     * @throws SQLException if a database error occurs
     */
    public static GalleryAttrCursor select(String clauses) throws SQLException {
        Connection conn = PersistCrawlerIf.getConnection();
        String sql = "SELECT name, good, bad FROM GalleryAttr";
        if (clauses != null && !clauses.isBlank()) sql += " " + clauses;
        PreparedStatement ps = conn.prepareStatement(sql);
        return new GalleryAttrCursor(ps, ps.executeQuery());
    }

    /**
     * Returns the number of GalleryAttr rows that match the given SQL clauses.
     *
     * <p>Any {@code ORDER BY} clause in {@code clauses} is stripped before counting.
     * The method closes its own statement and result set via try-with-resources.</p>
     *
     * @param clauses optional SQL {@code WHERE} (and other) clauses to filter the count,
     *                or {@code null} / blank to count all rows
     * @return the number of matching rows
     * @throws SQLException if a database error occurs
     */
    public static int count(String clauses) throws SQLException {
        Connection conn = PersistCrawlerIf.getConnection();
        String sql = "SELECT COUNT(*) FROM GalleryAttr";
        String filtered = clauses == null ? "" :
                clauses.replaceAll("(?i)\\s*ORDER\\s+BY\\s+[^;]*", "").trim();
        if (!filtered.isBlank()) sql += " " + filtered;
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    /**
     * Advances the cursor and returns the next {@link GalleryAttr}.
     *
     * <p>Boolean columns ({@code good}, {@code bad}) are read with {@code wasNull()}
     * to correctly distinguish SQL {@code NULL} from {@code FALSE}. When the result
     * set is exhausted, {@link #close()} is called automatically and {@code null}
     * is returned.</p>
     *
     * @return the next GalleryAttr, or {@code null} if there are no more rows
     * @throws SQLException if a database error occurs
     */
    public GalleryAttr next() throws SQLException {
        if (!rs.next()) {
            close();
            return null;
        }
        GalleryAttr bean = new GalleryAttr(rs.getString("name"));
        boolean good = rs.getBoolean("good"); bean.setGood(rs.wasNull() ? null : good);
        boolean bad  = rs.getBoolean("bad");  bean.setBad(rs.wasNull()  ? null : bad);
        return bean;
    }

    /**
     * Drains the cursor and returns all remaining rows as an array.
     *
     * <p>The cursor is closed automatically when iteration is complete.</p>
     *
     * @return an array of all remaining GalleryAttr rows; never {@code null}
     * @throws SQLException if a database error occurs
     */
    public GalleryAttr[] getAll() throws SQLException {
        List<GalleryAttr> list = new ArrayList<>();
        GalleryAttr row;
        while ((row = next()) != null) list.add(row);
        return list.toArray(new GalleryAttr[0]);
    }

    /**
     * Closes the underlying result set and prepared statement, suppressing any errors.
     *
     * <p>Safe to call more than once. Called automatically by {@link #next()} when
     * the result set is exhausted.</p>
     */
    public void close() {
        try { rs.close(); } catch (Exception ignored) {}
        try { ps.close(); } catch (Exception ignored) {}
    }
}
