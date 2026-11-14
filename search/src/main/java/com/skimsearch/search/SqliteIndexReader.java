package com.skimsearch.search;

import com.skimsearch.common.Book;
import com.skimsearch.common.Filters;
import com.skimsearch.common.IndexReader;
import com.skimsearch.common.IndexUnavailableException;
import com.skimsearch.common.SearchResult;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * IndexReader over one SQLite file (same schema as Stage 2: books + postings).
 * Design A: each Search node reads its own local copy of a published version.
 */
public class SqliteIndexReader implements IndexReader {

    private final Path dbFile;
    private final String jdbcUrl;
    private final long version;

    public SqliteIndexReader(Path dbFile, long version) {
        this.dbFile = dbFile.toAbsolutePath().normalize();
        this.jdbcUrl = "jdbc:sqlite:file:" + this.dbFile.toString().replace("\\", "/") + "?mode=ro";
        this.version = version;
    }

    @Override
    public List<SearchResult> search(String term, Filters filters, int offset, int limit) {

        List<Object> params = new ArrayList<>();
        String sql = """
                SELECT b.book_id, b.title, b.author, b.language, b.year, p.tf
                FROM postings p
                JOIN books b ON b.book_id = p.book_id
                WHERE p.term = ?
                """ + filterClause(term, filters, params)
                + " ORDER BY p.tf DESC, b.book_id ASC LIMIT ? OFFSET ?";
        params.add(limit);
        params.add(offset);

        try (Connection c = connect();
             PreparedStatement st = prepare(c, sql, params);
             ResultSet rs = st.executeQuery()) {

            List<SearchResult> results = new ArrayList<>();
            while (rs.next()) {
                results.add(new SearchResult(
                        rs.getInt("book_id"),
                        rs.getString("title"),
                        rs.getString("author"),
                        rs.getString("language"),
                        nullableInt(rs, "year"),
                        rs.getInt("tf")
                ));
            }
            return results;

        } catch (SQLException e) {
            throw new IndexUnavailableException("search failed on index v" + version, e);
        }
    }

    @Override
    public int count(String term, Filters filters) {

        List<Object> params = new ArrayList<>();
        String sql = """
                SELECT COUNT(*)
                FROM postings p
                JOIN books b ON b.book_id = p.book_id
                WHERE p.term = ?
                """ + filterClause(term, filters, params);

        try (Connection c = connect();
             PreparedStatement st = prepare(c, sql, params);
             ResultSet rs = st.executeQuery()) {

            return rs.next() ? rs.getInt(1) : 0;

        } catch (SQLException e) {
            throw new IndexUnavailableException("count failed on index v" + version, e);
        }
    }

    @Override
    public Optional<Book> findBook(int bookId) {

        String sql = """
                SELECT book_id, title, author, language, year, term_count, indexed_at
                FROM books
                WHERE book_id = ?
                """;

        try (Connection c = connect();
             PreparedStatement st = prepare(c, sql, List.of(bookId));
             ResultSet rs = st.executeQuery()) {

            if (!rs.next()) {
                return Optional.empty();
            }
            return Optional.of(new Book(
                    rs.getInt("book_id"),
                    rs.getString("title"),
                    rs.getString("author"),
                    rs.getString("language"),
                    nullableInt(rs, "year"),
                    rs.getInt("term_count"),
                    rs.getString("indexed_at")
            ));

        } catch (SQLException e) {
            throw new IndexUnavailableException("findBook failed on index v" + version, e);
        }
    }

    /** 0 if the file does not exist: nothing is loaded, so the node is not ready. */
    @Override
    public long servedVersion() {
        return Files.isRegularFile(dbFile) ? version : 0;
    }

    /** Builds the WHERE tail and fills params (term first, then the filters). */
    private static String filterClause(String term, Filters filters, List<Object> params) {
        params.add(term);
        StringBuilder sql = new StringBuilder();
        if (filters.author() != null) {
            sql.append(" AND LOWER(b.author) = LOWER(?)");
            params.add(filters.author());
        }
        if (filters.language() != null) {
            sql.append(" AND LOWER(b.language) = LOWER(?)");
            params.add(filters.language());
        }
        if (filters.year() != null) {
            sql.append(" AND b.year = ?");
            params.add(filters.year());
        }
        return sql.toString();
    }

    private Connection connect() throws SQLException {
        if (!Files.isRegularFile(dbFile)) {
            throw new SQLException("index file not found: " + dbFile);
        }
        return DriverManager.getConnection(jdbcUrl);
    }

    private static PreparedStatement prepare(Connection c, String sql, List<Object> params)
            throws SQLException {
        PreparedStatement st = c.prepareStatement(sql);
        for (int i = 0; i < params.size(); i++) {
            Object value = params.get(i);
            if (value instanceof Integer n) {
                st.setInt(i + 1, n);
            } else {
                st.setString(i + 1, value.toString());
            }
        }
        return st;
    }

    private static Integer nullableInt(ResultSet rs, String column) throws SQLException {
        return rs.getObject(column) == null ? null : rs.getInt(column);
    }
}