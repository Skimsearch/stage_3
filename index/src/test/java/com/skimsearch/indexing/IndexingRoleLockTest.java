package com.skimsearch.indexing;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Two Indexing roles on the same folder, guarded by the Hazelcast lock, never collide.
 * (The roles are in one JVM; the lock owner is the calling thread, so the threads exclude each other.) */
class IndexingRoleLockTest {

    private static final int ROUNDS = 10;

    @TempDir
    Path tmp;

    HazelcastInstance hz;
    final List<IndexingRole> roles = new ArrayList<>();
    Path indexDir;
    Path lake;

    @BeforeEach
    void setUp() throws IOException {
        Config config = new Config();
        config.setClusterName("test-" + UUID.randomUUID());
        config.setProperty("hazelcast.logging.type", "none");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        hz = Hazelcast.newHazelcastInstance(config);

        lake = tmp.resolve("datalake");
        Path batch = lake.resolve("batch").resolve("1000-1999");
        Files.createDirectories(batch);
        Files.writeString(batch.resolve("1342.header.txt"),
                "Title: Pride\nAuthor: Jane Austen\nLanguage: English\n");
        Files.writeString(batch.resolve("1342.body.txt"), "pride and prejudice");
        indexDir = tmp.resolve("index");
    }

    @AfterEach
    void tearDown() {
        roles.forEach(IndexingRole::close);
        hz.shutdown();
    }

    private IndexingRole newRole() {
        IndexingRole role = new IndexingRole(
                new VersionedIndexingService(new DatalakeReader(lake), new IndexVersionWriter(indexDir)),
                new IndexPublisher(hz, indexDir),
                Duration.ofSeconds(60),
                HazelcastBuildLock.of(hz));
        roles.add(role);
        return role;
    }

    @Test
    void twoRolesBuildingAtTheSameTimeProduceConsecutiveVersions() throws Exception {
        IndexingRole a = newRole();
        IndexingRole b = newRole();

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            List<Future<?>> results = new ArrayList<>();
            for (IndexingRole role : List.of(a, b)) {
                results.add(pool.submit(() -> {
                    for (int i = 0; i < ROUNDS; i++) {
                        role.indexBooks(List.of(1342));
                    }
                }));
            }
            for (Future<?> result : results) {
                result.get(60, TimeUnit.SECONDS); // an ExecutionException here means a collision
            }
        } finally {
            pool.shutdownNow();
        }

        assertEquals(2 * ROUNDS, new IndexPublisher(hz, indexDir).candidateVersion());
    }
}