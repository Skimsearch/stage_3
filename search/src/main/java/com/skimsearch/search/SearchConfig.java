package com.skimsearch.search;

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
 * CONSUL_URL set          -> registers in that Consul agent (normally http://127.0.0.1:8500),
 *                            advertising SEARCH_ADVERTISE_HOST, which is then required: the IP
 *                            other machines use to reach this Search (never guessed, because
 *                            on Linux the local host name often resolves to 127.0.1.1).
 * CONSUL_URL empty        -> no service discovery (local tests).
 * INDEX_DESIGN            A (default) or B. B reads the index from Hazelcast maps
 *                         (docs/design-b-contract.md) and needs a Hazelcast instance.
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
        String advertiseHost,
        String indexDesign
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
            
        URI consulUrl = env.getOrDefault("CONSUL_URL", "").isBlank()
                ? null
                : URI.create(env.get("CONSUL_URL"));
        String advertiseHost = env.getOrDefault("SEARCH_ADVERTISE_HOST", "").isBlank()
                ? null
                : env.get("SEARCH_ADVERTISE_HOST").trim();
        if (consulUrl != null && advertiseHost == null) {
            throw new IllegalArgumentException(
                    "CONSUL_URL is set but SEARCH_ADVERTISE_HOST is missing: set it to this machine's IP");
        }

        String indexDesign = env.getOrDefault("INDEX_DESIGN", "A").trim().toUpperCase(java.util.Locale.ROOT);
        if (indexDesign.isEmpty()) {
            indexDesign = "A";
        }
        if (!indexDesign.equals("A") && !indexDesign.equals("B")) {
            throw new IllegalArgumentException("INDEX_DESIGN must be A or B: " + env.get("INDEX_DESIGN"));
        }

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
                consulUrl,
                advertiseHost,
                indexDesign
        );
    }

    public boolean usesHazelcast() {
        return !hzAddresses.isEmpty();
    }

    public boolean designB() {
        return indexDesign.equals("B");
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