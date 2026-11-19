package com.skimsearch.indexing;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.skimsearch.common.HazelcastNames;
import com.skimsearch.common.IndexManifest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class IndexingRoleTest {

    @TempDir
    Path tmp;

    HazelcastInstance hz;
    IndexPublisher publisher;
    IndexingRole role;

    @BeforeEach
    void setUp() throws IOException {
        Config config = new Config();
        config.setClusterName("test-" + UUID.randomUUID());
        config.setProperty("hazelcast.logging.type", "none");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        hz = Hazelcast.newHazelcastInstance(config);

        Path lake = tmp.resolve("datalake");
        Path batch = lake.resolve("batch").resolve("1000-1999");
        Files.createDirectories(batch);
        Files.writeString(batch.resolve("1342.header.txt"),
                "Title: Pride\nAuthor: Jane Austen\nLanguage: English\n");
        Files.writeString(batch.resolve("1342.body.txt"), "pride and prejudice");

        publisher = new IndexPublisher(hz);
        role = new IndexingRole(
                new VersionedIndexingService(new DatalakeReader(lake), new IndexVersionWriter(tmp.resolve("index"))),
                publisher, Duration.ofMillis(100));
    }

    @AfterEach
    void tearDown() {
        role.close();
        hz.shutdown();
    }

    @Test
    void candidateIsPublishedOnceASearchHasLoadedIt() throws InterruptedException {
        IndexManifest manifest = role.indexBooks(List.of(1342));

        assertEquals(1, manifest.version());
        assertEquals(1, publisher.candidateVersion());
        assertEquals(0, publisher.publishedVersion());

        hz.<String, Long>getMap(HazelcastNames.LOADED_MAP).put("s1", 1L);

        long deadline = System.currentTimeMillis() + 3_000;
        while (publisher.publishedVersion() != 1 && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(1, publisher.publishedVersion());
    }
}