package com.skimsearch.search;

import com.hazelcast.core.HazelcastInstance;
import com.skimsearch.common.Book;
import com.skimsearch.common.Filters;
import com.skimsearch.common.IndexReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.skimsearch.search.DesignBFixture.Posting;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Same logical index in design A (SQLite) and design B (Hazelcast maps): every query must return
 * the same total and the same results in the same order (checked before comparing speed).
 */
class DesignEquivalenceTest {

    @TempDir
    Path dir;

    static final String[] AUTHORS = {"Jane Austen", "JANE AUSTEN", "Émile Zola", "émile zola", "Lewis Carroll", null};
    static final String[] LANGUAGES = {"en", "EN", "fr", "es"};
    static final String[] TERMS = {"love", "war", "sea", "the", "alice", "whale", "rare"};

    @Test
    void designsAAndBReturnIdenticalResults() throws Exception {
        Random random = new Random(42);
        List<Book> books = new ArrayList<>();
        List<Posting> postings = new ArrayList<>();
        for (int id = 1; id <= 40; id++) {
            Integer year = id % 7 == 0 ? null : 1990 + id % 5;
            books.add(new Book(id, "Title " + id, AUTHORS[id % AUTHORS.length], LANGUAGES[id % LANGUAGES.length],
                    year, 100 + id, "2026-11-24T10:00:00Z"));
            for (String term : TERMS) {
                if (term.equals("rare") && id != 17) {
                    continue;
                }
                if (term.equals("the") || random.nextInt(3) == 0) {
                    postings.add(new Posting(term, id, 1 + random.nextInt(4)));   // few values: many tf ties
                }
            }
        }

        IndexReader a = new SqliteIndexReader(writeSqlite(books, postings), 1);
        HazelcastInstance hz = DesignBFixture.member();
        try (HazelcastIndexReader bReader = new HazelcastIndexReader(hz, () -> 1, Duration.ofHours(1))) {
            DesignBFixture.writeVersion(hz, 1, books, postings);
            IndexReader b = bReader.snapshot();

            List<Filters> filters = new ArrayList<>(List.of(Filters.none()));
            for (String author : new String[]{"jane austen", "ÉMILE ZOLA", "émile zola", "lewis carroll"}) {
                filters.add(new Filters(author, null, null));
            }
            filters.add(new Filters(null, "EN", null));
            filters.add(new Filters(null, null, 1992));
            filters.add(new Filters("Jane Austen", "en", 1991));

            int compared = 0;
            for (String term : List.of("love", "war", "sea", "the", "alice", "whale", "rare", "absent")) {
                for (Filters f : filters) {
                    String what = term + " " + f;
                    assertEquals(a.count(term, f), b.count(term, f), "count " + what);
                    for (int[] page : new int[][]{{0, 20}, {0, 100}, {3, 5}, {35, 10}, {100, 10}}) {
                        assertEquals(a.search(term, f, page[0], page[1]), b.search(term, f, page[0], page[1]),
                                "search " + what + " offset " + page[0] + " limit " + page[1]);
                        compared++;
                    }
                }
            }
            for (int id = 0; id <= 41; id++) {
                assertEquals(a.findBook(id), b.findBook(id), "book " + id);
            }
            System.out.println("design A and B identical on " + compared + " searches and 42 book lookups");
        } finally {
            hz.shutdown();
        }
    }

    /** Same schema as design A (books + postings). */
    private Path writeSqlite(List<Book> books, List<Posting> postings) throws Exception {
        Path db = dir.resolve("index-v1.db");
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db);
             Statement st = c.createStatement()) {
            st.executeUpdate("""
                    CREATE TABLE books (book_id INTEGER PRIMARY KEY, title TEXT, author TEXT,
                      language TEXT, year INTEGER, term_count INTEGER, indexed_at TEXT)""");
            st.executeUpdate("CREATE TABLE postings (term TEXT, book_id INTEGER, tf INTEGER)");
            try (PreparedStatement ins = c.prepareStatement("INSERT INTO books VALUES (?,?,?,?,?,?,?)")) {
                for (Book b : books) {
                    ins.setInt(1, b.bookId());
                    ins.setString(2, b.title());
                    ins.setString(3, b.author());
                    ins.setString(4, b.language());
                    ins.setObject(5, b.year());
                    ins.setInt(6, b.termCount());
                    ins.setString(7, b.indexedAt());
                    ins.executeUpdate();
                }
            }
            try (PreparedStatement ins = c.prepareStatement("INSERT INTO postings VALUES (?,?,?)")) {
                for (Posting p : postings) {
                    ins.setString(1, p.term());
                    ins.setInt(2, p.bookId());
                    ins.setInt(3, p.tf());
                    ins.executeUpdate();
                }
            }
        }
        return db;
    }
}