package com.skimsearch.search;

import java.net.InetAddress;
import java.net.URI;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Search role configuration, from environment variables.
 *
 * HZ_ADDRESSES empty      -> no Hazelcast: the published version is PUBLISHED_VERSION (local tests).
 * HZ_ADDRESSES set        -> the published version comes from Hazelcast.
 * SHARED_INDEX_DIR empty  -> serves the single file SEARCH_INDEX_PATH as INDEX_VERSION (local tests).
 * SHARED_INDEX_DIR set    -> design A: local copies of the versions published in that folder.
 * CONSUL_URL empty        -> no service discovery (local tests).
 * CONSUL_URL set          -> registers in that Consul agent (normally http://127.0.0.1:8500),
 *                            advertising SEARCH_ADVERTISE_HOST (default: this machine's address).
 */
public record SearchConfig(
        String workerId,
        int port,
        int threads,
        Path indexPath,
        long indexVersion,
        long publishedVersion,
        List<String> hzAddresses,
        String hzClusterName,
        Path localDir,
        Path sharedIndexDir,
        URI consulUrl,
        String advertiseHost
) {

    public static SearchConfig fromEnvironment() {
        return from(System.getenv());
    }

    static SearchConfig from(Map<String, String> env) {

        int port = Integer.parseInt(env.getOrDefault("SEARCH_PORT", "7003"));

        Path dataDir = Path.of(env.getOrDefault("DATA_DIR", "data"));
        Path indexPath = Path.of(env.getOrDefault(
                "SEARCH_INDEX_PATH",
                dataDir.resolve("datamart").resolve("index.db").toString()
        ));

        List<String> hzAddresses = Arrays.stream(env.getOrDefault("HZ_ADDRESSES", "").split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();

        return new SearchConfig(
                env.getOrDefault("WORKER_ID", "search-" + port),
                port,
                Integer.parseInt(env.getOrDefault("SEARCH_THREADS", "250")),
                indexPath,
                Long.parseLong(env.getOrDefault("INDEX_VERSION", "1")),
                Long.parseLong(env.getOrDefault("PUBLISHED_VERSION", "1")),
                hzAddresses,
                env.getOrDefault("HZ_CLUSTER_NAME", "skimsearch"),
                Path.of(env.getOrDefault("LOCAL_DIR", "local")),
                env.getOrDefault("SHARED_INDEX_DIR", "").isBlank()
                        ? null
                        : Path.of(env.get("SHARED_INDEX_DIR")),
                env.getOrDefault("CONSUL_URL", "").isBlank()
                        ? null
                        : URI.create(env.get("CONSUL_URL")),
                env.getOrDefault("SEARCH_ADVERTISE_HOST", "").isBlank()
                        ? localAddress()
                        : env.get("SEARCH_ADVERTISE_HOST")
        );
    }

    /** Address other machines use to reach this one. Set SEARCH_ADVERTISE_HOST if the guess is wrong. */
    private static String localAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            return "127.0.0.1";
        }
    }

    public boolean usesHazelcast() {
        return !hzAddresses.isEmpty();
    }

    public boolean usesConsul() {
        return consulUrl != null;
    }

    public boolean usesVersionedIndex() {
        return sharedIndexDir != null;
    }

    /** Node-local folder where this worker keeps its copies of the index versions. */
    public Path localIndexDir() {
        return localDir.resolve(workerId + "-index");
    }

    /** Node-local file with the last published version this worker saw. */
    public Path lastPublishedFile() {
        return localDir.resolve(workerId + "-published-version.txt");
    }
}