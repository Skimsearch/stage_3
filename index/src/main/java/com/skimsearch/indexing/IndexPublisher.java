package com.skimsearch.indexing;

import com.google.gson.Gson;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.skimsearch.common.IndexManifest;

import java.util.function.LongSupplier;

/**
 * Two-step publication of an index version (design A).
 *
 * 1. announceCandidate: after the manifest is written, stores it in "index-versions"
 *    and raises index.candidateVersion.
 * 2. tryPromote: raises index.publishedVersion to the candidate once enough Searches
 *    have loaded it: max(1, min(2, active Searches)), or after 30 s if at least one has it.
 *
 * Versions never go down, and any Indexing worker may promote: the compare-and-set
 * (replace(key, old, new)) makes concurrent attempts harmless.
 * Map and key names must match the search module.
 */
public final class IndexPublisher {

    public static final String META_MAP = "index-meta";
    public static final String PUBLISHED_KEY = "index.publishedVersion";
    public static final String CANDIDATE_KEY = "index.candidateVersion";
    public static final String VERSIONS_MAP = "index-versions";
    public static final String LOADED_MAP = "search-loaded";

    static final long MAX_WAIT_MILLIS = 30_000;
    private static final Gson GSON = new Gson();

    private final IMap<String, Long> meta;
    private final IMap<Long, String> versions;
    private final IMap<String, Long> loaded;
    private final LongSupplier clock;

    public IndexPublisher(HazelcastInstance hz) {
        this(hz, System::currentTimeMillis);
    }

    /** The clock is a parameter so tests can move time forward. */
    IndexPublisher(HazelcastInstance hz, LongSupplier clock) {
        this.meta = hz.getMap(META_MAP);
        this.versions = hz.getMap(VERSIONS_MAP);
        this.loaded = hz.getMap(LOADED_MAP);
        this.clock = clock;
    }

    /** Step 1. Call it only after index-v{N}.db and its manifest are complete. */
    public void announceCandidate(IndexManifest manifest) {
        versions.put(manifest.version(), GSON.toJson(manifest));
        raise(CANDIDATE_KEY, manifest.version());
    }

    /** Step 2. Returns true if this call raised the published version. */
    public boolean tryPromote() {
        long candidate = candidateVersion();
        if (candidate <= publishedVersion()) {
            return false;
        }
        long active = 0;
        long withCandidate = 0;
        for (Long version : loaded.values()) {
            active++;
            if (version >= candidate) {
                withCandidate++;
            }
        }
        long needed = Math.max(1, Math.min(2, active));
        boolean enough = withCandidate >= needed;
        boolean timedOut = withCandidate >= 1 && waitedMillis(candidate) >= MAX_WAIT_MILLIS;
        if (!enough && !timedOut) {
            return false;
        }
        return raise(PUBLISHED_KEY, candidate);
    }

    public long publishedVersion() {
        return current(PUBLISHED_KEY);
    }

    public long candidateVersion() {
        return current(CANDIDATE_KEY);
    }

    private long waitedMillis(long version) {
        String json = versions.get(version);
        if (json == null) {
            return 0;
        }
        return clock.getAsLong() - GSON.fromJson(json, IndexManifest.class).createdAtMillis();
    }

    private long current(String key) {
        Long value = meta.get(key);
        return value == null ? 0 : value;
    }

    /** Sets key to value only if that raises it. Values are always Long (Search reads Long). */
    private boolean raise(String key, long value) {
        while (true) {
            Long current = meta.get(key);
            if (current == null) {
                if (meta.putIfAbsent(key, value) == null) {
                    return true;
                }
            } else if (current >= value) {
                return false;
            } else if (meta.replace(key, current, value)) {
                return true;
            }
        }
    }
}