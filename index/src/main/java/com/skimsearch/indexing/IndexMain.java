package com.skimsearch.indexing;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Standalone launcher of the indexing role, for tests on one or several machines
 * until the single worker program exists (it will call IndexingRole itself).
 *
 * It starts a Hazelcast member (the Search instances connect to it with HZ_ADDRESSES),
 * restores the keys from the shared folder, and every INDEX_EVERY_SECONDS indexes the
 * datalake books it has not indexed yet as one new version.
 *
 * Environment variables:
 *   SHARED_INDEX_DIR     required: folder shared by Indexing and every Search
 *   DATALAKE_DIR         default DATA_DIR/datalake
 *   DATA_DIR             default "data"
 *   HZ_CLUSTER_NAME      default "skimsearch"
 *   HZ_PORT              default 5701
 *   HZ_PUBLIC_ADDRESS    optional host:port other machines use to reach this member
 *   HZ_MEMBERS           optional comma-separated host:port list of the Hazelcast members
 *                        (this one included); enables the tcp-ip join so members on
 *                        different machines form ONE cluster. Without it, this member
 *                        runs alone.
 *   INDEX_DESIGN         A (default: SQLite files in SHARED_INDEX_DIR) or B (index in Hazelcast maps;
 *                        only published-version.txt, kept in SHARED_INDEX_DIR/design-b)
 *   INDEX_EVERY_SECONDS  default 10
 *   PROMOTE_EVERY_SECONDS default 1
 *
 * Building a version is guarded by a Hazelcast lock (HazelcastBuildLock), so two IndexMain
 * never write at the same time. Run a single one anyway until the task queue exists: two of them on
 * the same datalake would each index all the books, producing duplicate versions.
 *
 * After a restart it indexes every complete book again as a new version (re-indexing a
 * book replaces it, so the content is the same).
 */
public final class IndexMain {

    private IndexMain() {
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Map<String, String> env = System.getenv();

        String shared = env.getOrDefault("SHARED_INDEX_DIR", "");
        if (shared.isBlank()) {
            System.err.println("SHARED_INDEX_DIR is required");
            System.exit(2);
        }
        String design = IndexBuilders.normalise(env.get("INDEX_DESIGN"));
        Path sharedIndexDir = IndexBuilders.indexDir(design, Path.of(shared));
        Files.createDirectories(sharedIndexDir);

        Path dataDir = Path.of(env.getOrDefault("DATA_DIR", "data"));
        Path datalakeDir = Path.of(env.getOrDefault("DATALAKE_DIR", dataDir.resolve("datalake").toString()));
        Duration indexEvery = Duration.ofSeconds(Long.parseLong(env.getOrDefault("INDEX_EVERY_SECONDS", "10")));
        Duration promoteEvery = Duration.ofSeconds(Long.parseLong(env.getOrDefault("PROMOTE_EVERY_SECONDS", "1")));

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(hazelcastConfig(env));

        DatalakeReader datalake = new DatalakeReader(datalakeDir);
        IndexingRole role = new IndexingRole(
                IndexBuilders.create(design, datalake, sharedIndexDir, hz),
                new IndexPublisher(hz, sharedIndexDir),
                promoteEvery,
                HazelcastBuildLock.of(hz));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            role.close();
            hz.shutdown();
        }));

        log("event=index-main-started design=" + design + " datalake=" + datalakeDir.toAbsolutePath()
                + " sharedIndexDir=" + sharedIndexDir.toAbsolutePath());

        Set<Integer> indexed = new HashSet<>();
        while (true) {
            try {
                List<Integer> fresh = datalake.completeBookIds().stream()
                        .filter(id -> !indexed.contains(id))
                        .toList();
                if (!fresh.isEmpty()) {
                    role.indexBooks(fresh);
                    indexed.addAll(fresh);
                }
            } catch (RuntimeException e) {
                log("event=index-run-failed error=" + e);
            }
            Thread.sleep(indexEvery.toMillis());
        }
    }

    /** Package-private so that a test can check the join settings without starting Hazelcast. */
    static Config hazelcastConfig(Map<String, String> env) {
        Config config = new Config();
        config.setClusterName(env.getOrDefault("HZ_CLUSTER_NAME", "skimsearch"));
        config.setProperty("hazelcast.logging.type", "jdk");
        var network = config.getNetworkConfig();
        network.setPort(Integer.parseInt(env.getOrDefault("HZ_PORT", "5701")));
        network.setPortAutoIncrement(false);
        // design B: index maps keep one backup, so a member can leave without losing the index
        config.getMapConfig("postings-v*").setBackupCount(1);
        config.getMapConfig("books-v*").setBackupCount(1);
        String publicAddress = env.getOrDefault("HZ_PUBLIC_ADDRESS", "");
        if (!publicAddress.isBlank()) {
            network.setPublicAddress(publicAddress);
        }
        var join = network.getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        String members = env.getOrDefault("HZ_MEMBERS", "");
        if (!members.isBlank()) {
            var tcpIp = join.getTcpIpConfig();
            tcpIp.setEnabled(true);
            for (String member : members.split(",")) {
                if (!member.isBlank()) {
                    tcpIp.addMember(member.trim());
                }
            }
        }
        return config;
    }

    private static void log(String event) {
        System.out.println("ts=" + Instant.now() + " " + event);
    }
}