package com.skimsearch.indexing;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.skimsearch.common.HazelcastNames;
import com.skimsearch.common.IndexManifest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Restoring the Hazelcast keys from the shared folder after a restart. */
class IndexPublisherRestoreTest {

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

    private IndexManifest fakeVersion(long version) throws IOException {
        Path db = dir.resolve(IndexManifest.dbFileName(version));
        Files.writeString(db, "db " + version);
        return IndexManifestWriter.write(db, version);
    }

    @Test
    void restoresCandidateAndVersionsFromManifests() throws IOException {
        fakeVersion(1);
        fakeVersion(2);

        IndexPublisher publisher = new IndexPublisher(hz, dir);
        publisher.restore();

        assertEquals(2, publisher.candidateVersion());
        assertEquals(0, publisher.publishedVersion());
        assertEquals(2, hz.getMap(HazelcastNames.VERSIONS_MAP).size());
    }

    @Test
    void restoresThePublishedVersionSavedAtPromotion() throws IOException {
        IndexPublisher publisher = new IndexPublisher(hz, dir);
        publisher.announceCandidate(fakeVersion(1));
        IMap<String, Long> loaded = hz.getMap(HazelcastNames.LOADED_MAP);
        loaded.put("s1", 1L);
        assertTrue(publisher.tryPromote());

        // simulate a Hazelcast restart: everything in memory is gone
        hz.getMap(HazelcastNames.META_MAP).clear();
        hz.getMap(HazelcastNames.VERSIONS_MAP).clear();

        IndexPublisher after = new IndexPublisher(hz, dir);
        after.restore();

        assertEquals(1, after.candidateVersion());
        assertEquals(1, after.publishedVersion());
    }

    @Test
    void publishedFileNeverPointsBeyondTheManifestsThatExist() throws IOException {
        fakeVersion(1);
        Files.writeString(dir.resolve(IndexPublisher.PUBLISHED_FILE), "5");

        IndexPublisher publisher = new IndexPublisher(hz, dir);
        publisher.restore();

        assertEquals(1, publisher.publishedVersion());
    }

    @Test
    void restoreOnAnEmptyFolderChangesNothing() {
        IndexPublisher publisher = new IndexPublisher(hz, dir);
        publisher.restore();

        assertEquals(0, publisher.candidateVersion());
        assertEquals(0, publisher.publishedVersion());
    }
}