package com.skimsearch.indexing;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.skimsearch.common.IndexManifest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** Runs against a real single-member Hazelcast started inside the test JVM. */
class IndexPublisherTest {

    HazelcastInstance hz;
    IMap<String, Long> meta;
    IMap<String, Long> loaded;
    IndexPublisher publisher;
    long now = 1_000_000;

    @BeforeEach
    void startHazelcast() {
        Config config = new Config();
        config.setClusterName("test-" + UUID.randomUUID());
        config.setProperty("hazelcast.logging.type", "none");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        hz = Hazelcast.newHazelcastInstance(config);
        meta = hz.getMap(IndexPublisher.META_MAP);
        loaded = hz.getMap(IndexPublisher.LOADED_MAP);
        publisher = new IndexPublisher(hz, () -> now);
    }

    @AfterEach
    void stopHazelcast() {
        hz.shutdown();
    }

    private IndexManifest manifest(long version) {
        return new IndexManifest(version, IndexManifest.dbFileName(version), "a".repeat(64), 10, now);
    }

    @Test
    void candidateIsStoredAndNeverGoesDown() {
        publisher.announceCandidate(manifest(2));
        publisher.announceCandidate(manifest(1));

        assertEquals(2, publisher.candidateVersion());
        assertEquals(0, publisher.publishedVersion());
        assertEquals(2, hz.getMap(IndexPublisher.VERSIONS_MAP).size());
    }

    @Test
    void doesNotPromoteUntilASearchHasLoadedIt() {
        publisher.announceCandidate(manifest(1));
        loaded.put("s1", 0L);

        assertFalse(publisher.tryPromote());
        assertEquals(0, publisher.publishedVersion());
    }

    @Test
    void oneSearchIsEnoughWhenOnlyOneIsActive() {
        publisher.announceCandidate(manifest(1));
        loaded.put("s1", 1L);

        assertTrue(publisher.tryPromote());
        assertEquals(1, publisher.publishedVersion());
    }

    @Test
    void twoSearchesAreEnoughWhenThreeAreActive() {
        publisher.announceCandidate(manifest(1));
        loaded.put("s1", 1L);
        loaded.put("s2", 1L);
        loaded.put("s3", 0L);

        assertTrue(publisher.tryPromote());
        assertEquals(1, publisher.publishedVersion());
    }

    @Test
    void withOneOfTwoItWaitsThirtySeconds() {
        publisher.announceCandidate(manifest(1));
        loaded.put("s1", 1L);
        loaded.put("s2", 0L);

        assertFalse(publisher.tryPromote());
        now += IndexPublisher.MAX_WAIT_MILLIS;
        assertTrue(publisher.tryPromote());
        assertEquals(1, publisher.publishedVersion());
    }

    @Test
    void publishedNeverGoesDown() {
        meta.put(IndexPublisher.PUBLISHED_KEY, 5L);
        publisher.announceCandidate(manifest(3));
        loaded.put("s1", 3L);

        assertFalse(publisher.tryPromote());
        assertEquals(5, publisher.publishedVersion());
    }
}