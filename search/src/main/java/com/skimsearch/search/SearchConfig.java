package com.skimsearch.search;

import java.nio.file.Path;
import java.util.Map;

public record SearchConfig(
        String workerId,
        int port,
        int threads,
        Path indexPath,
        long indexVersion,
        long publishedVersion
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

        return new SearchConfig(
                env.getOrDefault("WORKER_ID", "search-" + port),
                port,
                Integer.parseInt(env.getOrDefault("SEARCH_THREADS", "250")),
                indexPath,
                Long.parseLong(env.getOrDefault("INDEX_VERSION", "1")),
                Long.parseLong(env.getOrDefault("PUBLISHED_VERSION", "1"))
        );
    }
}