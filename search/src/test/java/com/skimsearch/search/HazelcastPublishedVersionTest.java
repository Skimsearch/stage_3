package com.skimsearch.search;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.skimsearch.common.IndexUnavailableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.function.LongSupplier;

import static com.skimsearch.common.HazelcastNames.META_MAP;
import static com.skimsearch.common.HazelcastNames.PUBLISHED_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Runs against a real single-member Hazelcast started inside the test JVM. */
class HazelcastPublishedVersionTest {

    @TempDir
    Path dir;

    HazelcastInstance hz;
    IMap<String, Long> meta;

    @BeforeEach
    void startHazelcast() {
        Config config = new Config();
        config.setClusterName("test-" + UUID.randomUUID());
        config.setProperty("hazelcast.logging.type", "none");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        hz = Hazelcast.newHazelcastInstance(config);
        meta = hz.getMap(META_MAP);
    }

    @AfterEach
    void stopHazelcast() {
        if (hz.getLifecycleService().isRunning()) {
            hz.shutdown();
        }
    }

    private HazelcastPublishedVersion source() {
        return new HazelcastPublishedVersion(hz, dir.resolve("w1-published-version.txt"), Duration.ofSeconds(1));
    }

    @Test
    void unknownUntilSomethingIsPublished() {
        try (var source = source()) {
            assertThrows(IndexUnavailableException.class, source::publishedVersion);
            meta.put(PUBLISHED_KEY, 3L);
            assertEventually(3, source::publishedVersion);
        }
    }

    @Test
    void neverGoesDown() {
        meta.put(PUBLISHED_KEY, 5L);
        try (var source = source()) {
            assertEquals(5, source.publishedVersion());
            meta.put(PUBLISHED_KEY, 4L);
            source.poll();
            assertEquals(5, source.publishedVersion());
        }
    }

    @Test
    void savesLastKnownValueAndUsesItWhenHazelcastIsDown() throws Exception {
        meta.put(PUBLISHED_KEY, 7L);
        try (var source = source()) {
            assertEquals(7, source.publishedVersion());
        }
        assertEquals("7", Files.readString(dir.resolve("w1-published-version.txt")).trim());

        // Same worker restarted after the key was lost (e.g. Hazelcast restarted empty)
        meta.clear();
        try (var restarted = source()) {
            assertEquals(7, restarted.publishedVersion());
            hz.shutdown();
            restarted.poll();
            assertEquals(7, restarted.publishedVersion());
        }
    }

    private static void assertEventually(long expected, LongSupplier actual) {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline) {
            try {
                if (actual.getAsLong() == expected) {
                    return;
                }
            } catch (IndexUnavailableException ignored) {
                // not yet
            }
            Thread.onSpinWait();
        }
        assertEquals(expected, actual.getAsLong());
    }
}