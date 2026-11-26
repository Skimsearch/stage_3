package com.skimsearch.worker;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.skimsearch.common.Book;
import com.skimsearch.common.Filters;
import com.skimsearch.common.IndexManifest;
import com.skimsearch.common.IndexReader;
import com.skimsearch.indexing.DatalakeReader;
import com.skimsearch.indexing.HazelcastIndexingService;
import com.skimsearch.indexing.IndexPublisher;
import com.skimsearch.indexing.IndexVersionWriter;
import com.skimsearch.indexing.VersionedIndexingService;
import com.skimsearch.search.HazelcastIndexReader;
import com.skimsearch.search.SqliteIndexReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * End-to-end A/B equivalence with the real code: the same datalake indexed by both writers
 * (design A SQLite files, design B Hazelcast maps) and read by both readers must give the same
 * totals, the same results in the same order and the same books, version after version.
 * Lives in worker because it is the module that depends on both index and search.
 */
class DesignsAgreeEndToEndTest {

    @TempDir
    Path tmp;

    static final String[] AUTHORS = {"Jane Austen", "JANE AUSTEN", "Émile Zola", "émile zola", "Lewis Carroll"};
    static final String[] LANGUAGES = {"English", "French", "Spanish"};
    static final String[] WORDS = {"love", "war", "sea", "whale", "alice", "the", "rabbit", "garden"};

    @Test
    void designsAAndBAgreeAcrossVersions() throws Exception {
        Path lake = tmp.resolve("lake");
        Random random = new Random(7);
        for (int id = 1; id <= 24; id++) {
            writeBook(lake, id, random, id % 5 == 0 ? null : 1990 + id % 4);
        }
        Class.forName("org.sqlite.JDBC");
        DatalakeReader datalake = new DatalakeReader(lake);
        var writerA = new VersionedIndexingService(datalake, new IndexVersionWriter(tmp.resolve("a")));

        Config config = new Config();
        config.setClusterName("e2e-" + UUID.randomUUID());
        config.setProperty("hazelcast.logging.type", "none");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        try {
            var writerB = new HazelcastIndexingService(datalake, hz, Files.createDirectories(tmp.resolve("b")));
            AtomicLong publishedB = new AtomicLong();
            try (var readerB = new HazelcastIndexReader(hz, publishedB::get, Duration.ofHours(1))) {

                // version 1: books 1-16
                compare(build(writerA, writerB, hz, publishedB, range(1, 16)), readerB, publishedB);

                // version 2: books 17-24 are added, and book 3 is re-indexed with a new text
                writeBook(lake, 3, new Random(99), 1993);
                compare(build(writerA, writerB, hz, publishedB, add(range(17, 24), 3)), readerB, publishedB);
            }
        } finally {
            hz.shutdown();
        }
    }

    /** Builds the next version with both writers; returns design A's reader for it. */
    private IndexReader build(VersionedIndexingService writerA, HazelcastIndexingService writerB,
                              HazelcastInstance hz, AtomicLong publishedB, List<Integer> ids) {
        IndexManifest a = writerA.indexBooks(ids);
        IndexManifest b = writerB.indexBooks(ids);
        new IndexPublisher(hz).announceCandidate(b);   // manifest in index-versions: available in B
        publishedB.set(b.version());
        return new SqliteIndexReader(tmp.resolve("a").resolve(a.file()), a.version());
    }

    private static void compare(IndexReader a, HazelcastIndexReader readerB, AtomicLong publishedB) {
        IndexReader b = readerB.snapshot();
        assertEquals(publishedB.get(), b.servedVersion());

        List<Filters> filters = new ArrayList<>(List.of(Filters.none(),
                new Filters("ÉMILE ZOLA", null, null), new Filters("émile zola", null, null),
                new Filters("jane austen", null, null), new Filters(null, "EN", null),
                new Filters(null, "fr", null), new Filters(null, null, 1991), new Filters("Lewis Carroll", "en", 1992)));
        List<String> terms = new ArrayList<>(List.of(WORDS));
        terms.add("absent");
        for (String term : terms) {
            for (Filters f : filters) {
                assertEquals(a.count(term, f), b.count(term, f), "count " + term + " " + f);
                for (int[] page : new int[][]{{0, 20}, {0, 100}, {2, 5}, {50, 10}}) {
                    assertEquals(a.search(term, f, page[0], page[1]), b.search(term, f, page[0], page[1]),
                            "search " + term + " " + f + " offset " + page[0] + " limit " + page[1]);
                }
            }
        }
        for (int id = 0; id <= 25; id++) {
            assertEquals(a.findBook(id).map(DesignsAgreeEndToEndTest::withoutIndexedAt),
                    b.findBook(id).map(DesignsAgreeEndToEndTest::withoutIndexedAt), "book " + id);
        }
    }

    /** indexedAt is when each writer ran, so it legitimately differs between designs. */
    private static Book withoutIndexedAt(Book b) {
        return new Book(b.bookId(), b.title(), b.author(), b.language(), b.year(), b.termCount(), "");
    }

    private static void writeBook(Path lake, int id, Random random, Integer year) throws Exception {
        Path folder = Files.createDirectories(lake.resolve("batch").resolve("0-999"));
        String header = "Title: Book " + id + "\nAuthor: " + AUTHORS[id % AUTHORS.length]
                + "\nLanguage: " + LANGUAGES[id % LANGUAGES.length] + "\n"
                + (year == null ? "" : "Release date: June 1, " + year + " [eBook #" + id + "]\n");
        StringBuilder body = new StringBuilder("unique" + (char) ('a' + id % 26) + " ");
        for (String word : WORDS) {
            int times = word.equals("the") ? 1 + random.nextInt(3) : random.nextInt(4);   // many tf ties
            body.append((word + " ").repeat(times));
        }
        Files.writeString(folder.resolve(id + ".header.txt"), header);
        Files.writeString(folder.resolve(id + ".body.txt"), body.toString());
    }

    private static List<Integer> range(int from, int to) {
        List<Integer> ids = new ArrayList<>();
        for (int i = from; i <= to; i++) {
            ids.add(i);
        }
        return ids;
    }

    private static List<Integer> add(List<Integer> ids, int id) {
        ids.add(id);
        return ids;
    }
}