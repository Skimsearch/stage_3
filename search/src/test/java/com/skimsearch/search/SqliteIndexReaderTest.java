package com.skimsearch.search;

import com.skimsearch.common.Filters;
import com.skimsearch.common.IndexUnavailableException;
import com.skimsearch.common.SearchResult;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SqliteIndexReaderTest {

    @TempDir
    static Path dir;
    static Path db;

    @BeforeAll
    static void createIndex() throws Exception {
        db = dir.resolve("index-v1.db");
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db);
             Statement st = c.createStatement()) {
            st.executeUpdate("""
                    CREATE TABLE books (book_id INTEGER PRIMARY KEY, title TEXT, author TEXT,
                      language TEXT, year INTEGER, term_count INTEGER, indexed_at TEXT)""");
            st.executeUpdate("CREATE TABLE postings (term TEXT, book_id INTEGER, tf INTEGER)");
            st.executeUpdate("""
                    INSERT INTO books VALUES
                      (10, 'B10', 'Ann', 'en', 2000, 5, 't'),
                      (20, 'B20', 'Bob', 'en', 2001, 5, 't'),
                      (30, 'B30', 'Ann', 'es', 2000, 5, 't'),
                      (40, 'B40', 'Émile Zola', 'fr', 1880, 5, 't')""");
            st.executeUpdate("""
                    INSERT INTO postings VALUES
                      ('love', 30, 7), ('love', 20, 9), ('love', 10, 7), ('love', 40, 1)""");
        }
    }

    @Test
    void ordersByTfDescThenBookIdAsc() {
        var reader = new SqliteIndexReader(db, 1);
        List<SearchResult> r = reader.search("love", Filters.none(), 0, 10);
        assertEquals(List.of(20, 10, 30, 40), r.stream().map(SearchResult::bookId).toList());
        assertEquals(4, reader.count("love", Filters.none()));
    }

    @Test
    void appliesFiltersCaseInsensitively() {
        var reader = new SqliteIndexReader(db, 1);
        var r = reader.search("love", new Filters("ANN", null, 2000), 0, 10);
        assertEquals(List.of(10, 30), r.stream().map(SearchResult::bookId).toList());
    }

    @Test
    void missingIndexThrowsInsteadOfEmptyList() {
        var reader = new SqliteIndexReader(dir.resolve("missing.db"), 1);
        assertEquals(0, reader.servedVersion());
        assertThrows(IndexUnavailableException.class,
                () -> reader.search("love", Filters.none(), 0, 10));
    }

    /** Design A (SQLite LOWER) and design B (Filters.sameText) must agree, accents included. */
    @Test
    void sqliteAuthorFilterAgreesWithSameText() {
        var reader = new SqliteIndexReader(db, 1);
        for (String author : List.of("ÉMILE ZOLA", "émile zola", "Émile Zola", "EMILE ZOLA")) {
            boolean sqlite = reader.count("love", new Filters(author, null, null)) == 1;
            boolean java = Filters.sameText(author, "Émile Zola");
            assertEquals(java, sqlite, "author filter differs for: " + author);
        }
    }
}