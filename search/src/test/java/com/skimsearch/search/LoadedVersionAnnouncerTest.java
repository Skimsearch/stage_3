package com.skimsearch.search;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static com.skimsearch.common.HazelcastNames.LOADED_MAP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Runs against a real single-member Hazelcast started inside the test JVM. */
class LoadedVersionAnnouncerTest {

    HazelcastInstance hz;
    IMap<String, Long> loaded;

    @BeforeEach
    void startHazelcast() {
        Config config = new Config();
        config.setClusterName("test-" + UUID.randomUUID());
        config.setProperty("hazelcast.logging.type", "none");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        hz = Hazelcast.newHazelcastInstance(config);
        loaded = hz.getMap(LOADED_MAP);
    }

    @AfterEach
    void stopHazelcast() {
        hz.shutdown();
    }

    @Test
    void announcesAndUpdatesTheHighestLoadedVersion() {
        AtomicLong highest = new AtomicLong(0);
        try (var announcer = new LoadedVersionAnnouncer(hz, "w1", highest::get,
                Duration.ofHours(1), Duration.ofSeconds(10))) {
            assertEquals(0L, loaded.get("w1"), "active even with nothing loaded");
            highest.set(5);
            announcer.announce();
            assertEquals(5L, loaded.get("w1"));
        }
    }

    @Test
    void cleanShutdownRemovesTheEntry() {
        new LoadedVersionAnnouncer(hz, "w1", () -> 3, Duration.ofHours(1), Duration.ofSeconds(10)).close();
        assertFalse(loaded.containsKey("w1"));
    }

    @Test
    void entryExpiresIfTheNodeStopsRenewingIt() throws Exception {
        // never renewed (period 1 h): like a node that crashed without a clean shutdown
        new LoadedVersionAnnouncer(hz, "w1", () -> 3, Duration.ofHours(1), Duration.ofSeconds(1));
        assertEquals(3L, loaded.get("w1"));
        Thread.sleep(2500);
        assertNull(loaded.get("w1"), "expired by TTL");
    }
}