package com.skimsearch.indexing;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.skimsearch.common.HazelcastNames;
import com.skimsearch.common.IndexManifest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** published-version.txt must already hold the version when index.publishedVersion appears. */
class IndexPublisherOrderTest {

    @TempDir
    Path dir;

    HazelcastInstance hz;

    @BeforeEach
    void startHazelcast() {
        Config config = new Config();
        config.setClusterName("test-" + UUID.randomUUID());
        config.setProperty("hazelcast.logging.type", "none");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        hz = Hazelcast.newHazelcastInstance(config);
    }

    @AfterEach
    void stopHazelcast() {
        hz.shutdown();
    }

    @Test
    void fileIsWrittenBeforeThePublishedKeyIsRaised() throws Exception {
        IndexPublisher publisher = new IndexPublisher(hz, dir);
        IMap<String, Long> meta = hz.getMap(HazelcastNames.META_MAP);

        AtomicLong fileWhenKeyAppeared = new AtomicLong(-1);
        CountDownLatch seen = new CountDownLatch(1);
        meta.addEntryListener((EntryAddedListener<String, Long>) event -> {
            if (HazelcastNames.PUBLISHED_KEY.equals(event.getKey())) {
                try {
                    fileWhenKeyAppeared.set(Long.parseLong(
                            Files.readString(dir.resolve(IndexPublisher.PUBLISHED_FILE)).trim()));
                } catch (Exception e) {
                    fileWhenKeyAppeared.set(-1);
                }
                seen.countDown();
            }
        }, true);

        publisher.announceCandidate(new IndexManifest(1, "index-v1.db", "0".repeat(64), 0, System.currentTimeMillis()));
        hz.<String, Long>getMap(HazelcastNames.LOADED_MAP).put("s1", 1L);

        assertTrue(publisher.tryPromote());
        assertTrue(seen.await(5, TimeUnit.SECONDS));
        assertEquals(1, fileWhenKeyAppeared.get());
        assertEquals(1, publisher.publishedVersion());
    }
}
