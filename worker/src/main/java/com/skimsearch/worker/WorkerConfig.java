package com.skimsearch.worker;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public record WorkerConfig(
        String workerId,
        Set<String> roles,
        String runId,
        String hzClusterName,
        int hzPort,
        String hzPublicAddress,
        List<String> hzMembers,
        Path datalakeDir,
        Path sharedIndexDir,
        Path localDir
) {
    public static WorkerConfig fromEnvironment() {
        return fromMap(System.getenv());
    }

    public static WorkerConfig fromMap(Map<String, String> env) {
        String workerId = getOrDefault(env, "WORKER_ID", "worker-" + UUID.randomUUID().toString().substring(0, 6));

        String rawRoles = getOrDefault(env, "WORKER_ROLES", "search");
        Set<String> roles = Arrays.stream(rawRoles.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(s -> s.toLowerCase(Locale.ROOT))
                .collect(Collectors.toCollection(LinkedHashSet::new));

        if (roles.isEmpty()) {
            throw new IllegalArgumentException("WORKER_ROLES must define at least one role (crawl, index, search)");
        }

        String runId = getOrDefault(env, "RUN_ID", "local-dev");
        String hzClusterName = getOrDefault(env, "HZ_CLUSTER_NAME", "skimsearch");
        int hzPort = Integer.parseInt(getOrDefault(env, "HZ_PORT", "5701"));
        String hzPublicAddress = env.getOrDefault("HZ_PUBLIC_ADDRESS", "").trim();

        String rawMembers = getOrDefault(env, "HZ_MEMBERS", "127.0.0.1");
        List<String> hzMembers = Arrays.stream(rawMembers.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();

        Path datalakeDir = Path.of(getOrDefault(env, "DATALAKE_DIR", "data/datalake"));
        Path sharedIndexDir = Path.of(getOrDefault(env, "SHARED_INDEX_DIR", "data/shared-index"));
        Path localDir = Path.of(getOrDefault(env, "LOCAL_DIR", "data/local/" + workerId));

        return new WorkerConfig(
                workerId,
                Collections.unmodifiableSet(roles),
                runId,
                hzClusterName,
                hzPort,
                hzPublicAddress.isEmpty() ? null : hzPublicAddress,
                hzMembers,
                datalakeDir,
                sharedIndexDir,
                localDir
        );
    }

    private static String getOrDefault(Map<String, String> env, String key, String defaultValue) {
        String val = env.get(key);
        return (val == null || val.isBlank()) ? defaultValue : val.trim();
    }
}