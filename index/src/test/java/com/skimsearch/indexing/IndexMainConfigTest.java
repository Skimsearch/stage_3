package com.skimsearch.indexing;

import com.hazelcast.config.Config;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Join settings of the Hazelcast member started by IndexMain (no network needed). */
class IndexMainConfigTest {

    @Test
    void memberListEnablesTcpIpJoin() {
        Config config = IndexMain.hazelcastConfig(Map.of("HZ_MEMBERS", "10.0.0.1:5701, 10.0.0.2:5701"));

        var join = config.getNetworkConfig().getJoin();
        assertTrue(join.getTcpIpConfig().isEnabled());
        assertEquals(List.of("10.0.0.1:5701", "10.0.0.2:5701"), join.getTcpIpConfig().getMembers());
        assertFalse(join.getMulticastConfig().isEnabled());
        assertFalse(join.getAutoDetectionConfig().isEnabled());
    }

    @Test
    void withoutMemberListNoJoinMethodIsEnabled() {
        Config config = IndexMain.hazelcastConfig(Map.of());

        var join = config.getNetworkConfig().getJoin();
        assertFalse(join.getTcpIpConfig().isEnabled());
        assertFalse(join.getMulticastConfig().isEnabled());
        assertEquals("skimsearch", config.getClusterName());
    }
}