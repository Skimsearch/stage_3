package com.skimsearch.indexing;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.skimsearch.common.HazelcastNames;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class VersionRetentionTest {

    @Test
    void keepsPublishedPreviousAndNewerVersions() {
        assertEquals(List.of(1L), VersionRetention.deletable(Set.of(1L, 2L, 3L), 3));
        assertEquals(List.of(1L, 2L), VersionRetention.deletable(Set.of(1L, 2L, 3L, 4L), 4));
    }

    @Test
    void keepsTheCandidatesAbovePublished() {
        // published 3: keep 3, its previous (2) and the unpublished 4 and 5
        assertEquals(List.of(1L), VersionRetention.deletable(Set.of(1L, 2L, 3L, 4L, 5L), 3));
    }

    @Test
    void nothingIsDeletableUntilSomethingIsPublished() {
        assertEquals(List.of(), VersionRetention.deletable(Set.of(1L, 2L), 0));
    }

    @Test
    void numbersMaySkipAfterARestart() {
        // after a full restart of design B the numbering jumps from 1 to 6: keep 1 as the previous one
        assertEquals(List.of(), VersionRetention.deletable(Set.of(1L, 6L), 6));
        assertEquals(List.of(1L), VersionRetention.deletable(Set.of(1L, 6L, 9L), 9));
    }

    @Test
    void firstPublishedVersionDeletesNothing() {
        assertEquals(List.of(), VersionRetention.deletable(Set.of(1L), 1));
    }

    @Test
    void readsVersionsAndPublishedFromHazelcast() {
        Config config = new Config();
        config.setClusterName("test-" + UUID.randomUUID());
        config.setProperty("hazelcast.logging.type", "none");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        try {
            for (long v = 1; v <= 4; v++) {
                hz.<Long, String>getMap(HazelcastNames.VERSIONS_MAP).put(v, "{}");
            }
            hz.<String, Long>getMap(HazelcastNames.META_MAP).put(HazelcastNames.PUBLISHED_KEY, 3L);

            assertEquals(List.of(1L), VersionRetention.deletable(hz));
        } finally {
            hz.shutdown();
        }
    }
}
