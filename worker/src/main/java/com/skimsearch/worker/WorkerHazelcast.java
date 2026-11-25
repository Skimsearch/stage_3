package com.skimsearch.worker;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;

public final class WorkerHazelcast {

    private WorkerHazelcast() {
    }

    public static Config config(WorkerConfig workerConfig) {
        Config config = new Config();
        config.setClusterName(workerConfig.hzClusterName());
        config.setInstanceName("worker-" + workerConfig.workerId());

        NetworkConfig network = config.getNetworkConfig();
        network.setPort(workerConfig.hzPort());
        network.setPortAutoIncrement(false);

        if (workerConfig.hzPublicAddress() != null) {
            network.setPublicAddress(workerConfig.hzPublicAddress());
        }

        JoinConfig join = network.getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);

        join.getTcpIpConfig().setEnabled(true);
        for (String member : workerConfig.hzMembers()) {
            join.getTcpIpConfig().addMember(member);
        }

        // design B: index maps keep one backup, so a member can leave without losing the index
        config.getMapConfig("postings-v*").setBackupCount(1);
        config.getMapConfig("books-v*").setBackupCount(1);

        return config;
    }
}