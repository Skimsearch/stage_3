package com.skimsearch.search;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Search role configuration, from environment variables.
 *
 * HZ_ADDRESSES empty  -> no Hazelcast: the published version is PUBLISHED_VERSION (local tests).
 * HZ_ADDRESSES set    -> the published version comes from Hazelcast.
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
        Path localDir
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
                Path.of(env.getOrDefault("LOCAL_DIR", "local"))
        );
    }

    public boolean usesHazelcast() {
        return !hzAddresses.isEmpty();
    }

    /** Node-local file with the last published version this worker saw. */
    public Path lastPublishedFile() {
        return localDir.resolve(workerId + "-published-version.txt");
    }
}