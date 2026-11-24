package com.skimsearch.indexing;

import com.google.gson.Gson;
import com.hazelcast.config.Config;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.skimsearch.common.Book;
import com.skimsearch.common.HazelcastNames;
import com.skimsearch.common.IndexManifest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Design B writer: contract in docs/design-b-contract.md. */
class HazelcastIndexingServiceTest {

    @TempDir
    Path tmp;

    HazelcastInstance hz;
    Path lake;
    Path shared;
    HazelcastIndexingService service;

    @BeforeEach
    void setUp() throws IOException {
        Config config = new Config();
        config.setClusterName("test-" + UUID.randomUUID());
        config.setProperty("hazelcast.logging.type", "none");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        hz = Hazelcast.newHazelcastInstance(config);

        lake = tmp.resolve("datalake");
        shared = tmp.resolve("shared");
        Files.createDirectories(shared);
        book(1, "Pride", "Jane Austen", "darcy darcy love");
        book(2, "Moby", "Herman Melville", "whale love love darcy");
        book(3, "Third", "Somebody", "love whale");
        service = new HazelcastIndexingService(new DatalakeReader(lake), hz, shared);
    }

    @AfterEach
    void tearDown() {
        hz.shutdown();
    }

    private void book(int id, String title, String author, String body) throws IOException {
        Path folder = lake.resolve("batch").resolve("0-999");
        Files.createDirectories(folder);
        Files.writeString(folder.resolve(id + ".header.txt"),
                "Title: " + title + "\nAuthor: " + author + "\nLanguage: English\n");
        Files.writeString(folder.resolve(id + ".body.txt"), body);
    }

    /** Pretends that Indexing announced the manifest, which is what makes a version "previous". */
    private void announce(IndexManifest manifest) {
        hz.<Long, String>getMap(HazelcastNames.VERSIONS_MAP).put(manifest.version(), new Gson().toJson(manifest));
    }

    private IMap<String, int[]> postings(long version) {
        return hz.getMap(HazelcastNames.postingsMap(version));
    }

    private IMap<Integer, String> books(long version) {
        return hz.getMap(HazelcastNames.booksMap(version));
    }

    @Test
    void firstVersionHasTheBooksAndPostingsInContractOrder() {
        IndexManifest manifest = service.indexBooks(List.of(1, 2));

        assertEquals(1, manifest.version());
        assertEquals(2, service.booksInLastBuild(2));
        assertEquals("postings-v1", manifest.file());
        assertEquals(Set.of(1, 2), books(1).keySet());
        assertEquals(Set.of("darcy", "love", "whale"), postings(1).keySet());
        // tf DESC, then bookId ASC
        assertArrayEquals(new int[] {1, 2, 2, 1}, postings(1).get("darcy"));
        assertArrayEquals(new int[] {2, 2, 1, 1}, postings(1).get("love"));
        assertArrayEquals(new int[] {2, 1}, postings(1).get("whale"));

        Book stored = new Gson().fromJson(books(1).get(1), Book.class);
        assertEquals("Pride", stored.title());
        assertEquals("Jane Austen", stored.author());
        assertEquals(2, stored.termCount());
    }

    @Test
    void nextVersionAddsBooksAndReplacesReindexedOnesWithoutTouchingTheOldVersion() throws IOException {
        announce(service.indexBooks(List.of(1, 2)));
        book(1, "Pride", "Jane Austen", "darcy elizabeth");

        IndexManifest second = service.indexBooks(List.of(3, 1));

        assertEquals(2, second.version());
        // version 1 is immutable
        assertArrayEquals(new int[] {1, 2, 2, 1}, postings(1).get("darcy"));
        assertEquals(Set.of(1, 2), books(1).keySet());
        // version 2 = version 1 + book 3, with book 1 replaced
        assertEquals(Set.of(1, 2, 3), books(2).keySet());
        assertArrayEquals(new int[] {1, 1, 2, 1}, postings(2).get("darcy"));
        assertArrayEquals(new int[] {2, 2, 3, 1}, postings(2).get("love"));
        assertArrayEquals(new int[] {2, 1, 3, 1}, postings(2).get("whale"));
        assertArrayEquals(new int[] {1, 1}, postings(2).get("elizabeth"));
    }

    @Test
    void manifestHashMatchesAnIndependentComputationOverTheMaps() throws Exception {
        IndexManifest manifest = service.indexBooks(List.of(1, 2, 3));

        MessageDigest sha = MessageDigest.getInstance("SHA-256");
        long size = 0;
        for (String term : new TreeSet<>(postings(1).keySet())) {
            StringBuilder line = new StringBuilder(term).append('\t');
            int[] values = postings(1).get(term);
            for (int i = 0; i < values.length; i++) {
                line.append(i > 0 ? "," : "").append(values[i]);
            }
            byte[] bytes = line.append('\n').toString().getBytes(StandardCharsets.UTF_8);
            sha.update(bytes);
            size += bytes.length;
        }
        for (int id : new TreeSet<>(books(1).keySet())) {
            byte[] bytes = (id + "\t" + books(1).get(id) + "\n").getBytes(StandardCharsets.UTF_8);
            sha.update(bytes);
            size += bytes.length;
        }

        assertEquals(HexFormat.of().formatHex(sha.digest()), manifest.sha256());
        assertEquals(size, manifest.sizeBytes());
    }

    @Test
    void numberingContinuesAfterAFullRestartFromPublishedVersionFile() throws IOException {
        // Hazelcast is empty (full restart), but the shared folder remembers that Searches saw version 5
        Files.writeString(shared.resolve(IndexPublisher.PUBLISHED_FILE), "5");

        IndexManifest manifest = service.indexBooks(List.of(1, 2, 3));

        assertEquals(6, manifest.version());
        assertEquals(Set.of(1, 2, 3), books(6).keySet());
    }

    @Test
    void afterAHazelcastRestartTheNextVersionStillHoldsEveryBook() throws IOException {
        announce(service.indexBooks(List.of(1, 2, 3)));
        // Searches saw version 1, and the shared folder remembers it
        Files.writeString(shared.resolve(IndexPublisher.PUBLISHED_FILE), "1");
        // the cluster restarts while this indexer process keeps running: every map is gone
        hz.getDistributedObjects().forEach(DistributedObject::destroy);
        book(4, "Fourth", "Someone", "love darcy");

        IndexManifest manifest = service.indexBooks(List.of(4));

        assertEquals(2, manifest.version());
        assertEquals(4, service.booksInLastBuild(1));
        assertEquals(Set.of(1, 2, 3, 4), books(2).keySet());
        assertArrayEquals(new int[] {2, 2, 1, 1, 3, 1, 4, 1}, postings(2).get("love"));
        assertArrayEquals(new int[] {1, 2, 2, 1, 4, 1}, postings(2).get("darcy"));
    }

    @Test
    void numberingNeverReusesTheCandidateOrPublishedKeys() {
        hz.<String, Long>getMap(HazelcastNames.META_MAP).put(HazelcastNames.CANDIDATE_KEY, 9L);

        assertEquals(10, service.indexBooks(List.of(1)).version());
    }

    @Test
    void missingBookFailsBeforeAnyMapIsFilled() {
        assertThrows(IndexingService.BookNotFoundException.class, () -> service.indexBooks(List.of(1, 999)));

        assertTrue(postings(1).isEmpty());
        assertTrue(books(1).isEmpty());
    }

    @Test
    void emptyListIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> service.indexBooks(List.of()));
    }

    @Test
    void retryAfterAFailedBuildStartsFromCleanMaps() {
        // leftovers of a build that died before announcing its manifest
        postings(1).put("stale", new int[] {99, 1});
        books(1).put(99, "{}");

        service.indexBooks(List.of(1));

        assertFalse(postings(1).containsKey("stale"));
        assertEquals(Set.of(1), books(1).keySet());
    }

    @Test
    void mergeKeepsContractOrderAndDropsReplacedBooks() {
        // book 1 tf 5, book 2 tf 3, book 3 tf 3; book 2 is replaced; books 4 (tf 3) and 5 (tf 9) are new
        int[] old = {1, 5, 2, 3, 3, 3};

        int[] merged = HazelcastIndexingService.mergePostings(old, Set.of(2),
                List.of(new int[] {4, 3}, new int[] {5, 9}));

        assertArrayEquals(new int[] {5, 9, 1, 5, 3, 3, 4, 3}, merged);
    }

    @Test
    void indexingRoleAnnouncesTheBuiltVersion() {
        IndexPublisher publisher = new IndexPublisher(hz);
        try (IndexingRole role = new IndexingRole(service, publisher, Duration.ofMillis(100))) {
            IndexManifest manifest = role.indexBooks(List.of(1, 2));

            assertEquals(1, publisher.candidateVersion());
            assertEquals(manifest.version(), publisher.candidateVersion());
        }
    }
}