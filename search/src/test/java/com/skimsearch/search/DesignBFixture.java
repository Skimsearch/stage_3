package com.skimsearch.search;

import com.google.gson.Gson;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.skimsearch.common.Book;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.skimsearch.common.HazelcastNames.VERSIONS_MAP;
import static com.skimsearch.common.HazelcastNames.booksMap;
import static com.skimsearch.common.HazelcastNames.postingsMap;

/** Writes a design B version by hand, exactly as the contract says (docs/design-b-contract.md). */
final class DesignBFixture {

    private DesignBFixture() {
    }

    static HazelcastInstance member() {
        Config c = new Config();
        c.setClusterName("test-" + UUID.randomUUID());
        c.setProperty("hazelcast.logging.type", "none");
        c.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        return Hazelcast.newHazelcastInstance(c);
    }

    /** One posting: book bookId contains term tf times. */
    record Posting(String term, int bookId, int tf) {
    }

    /**
     * Fills postings-v{version} and books-v{version}, then announces the version in index-versions
     * (the manifest value is not read by Search, so a placeholder is enough).
     */
    static void writeVersion(HazelcastInstance hz, long version, List<Book> books, List<Posting> postings) {
        Map<String, List<int[]>> byTerm = new HashMap<>();
        for (Posting p : postings) {
            byTerm.computeIfAbsent(p.term(), k -> new ArrayList<>()).add(new int[]{p.bookId(), p.tf()});
        }
        Map<String, int[]> values = new HashMap<>();
        byTerm.forEach((term, pairs) -> {
            pairs.sort((a, b) -> a[1] != b[1] ? Integer.compare(b[1], a[1]) : Integer.compare(a[0], b[0]));
            int[] flat = new int[pairs.size() * 2];
            for (int i = 0; i < pairs.size(); i++) {
                flat[2 * i] = pairs.get(i)[0];
                flat[2 * i + 1] = pairs.get(i)[1];
            }
            values.put(term, flat);
        });
        hz.<String, int[]>getMap(postingsMap(version)).putAll(values);

        Gson gson = new Gson();
        Map<Integer, String> json = new HashMap<>();
        books.forEach(b -> json.put(b.bookId(), gson.toJson(b)));
        hz.<Integer, String>getMap(booksMap(version)).putAll(json);

        hz.<Long, String>getMap(VERSIONS_MAP).put(version, "{\"version\":" + version + "}");
    }

    static Book book(int id, String author, String language, Integer year) {
        return new Book(id, "Title " + id, author, language, year, 10, "2026-11-24T10:00:00Z");
    }
}