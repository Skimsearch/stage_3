package com.skimsearch.indexing;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

import static com.skimsearch.common.HazelcastNames.META_MAP;
import static com.skimsearch.common.HazelcastNames.PUBLISHED_KEY;
import static com.skimsearch.common.HazelcastNames.VERSIONS_MAP;

/**
 * Which index versions can be deleted (design A files, design B maps).
 *
 * Keeps the published version N, the highest version below N (so requests still running on it can
 * finish) and every version above N (candidates not published yet). Everything older can be deleted.
 * Nothing is deleted until some version has been published. The function only decides: whoever
 * deletes (the cleanup of the worker) removes the files of design A, or destroys the two maps of
 * design B and removes the entry of "index-versions".
 */
public final class VersionRetention {

    private VersionRetention() {
    }

    /** Versions that can be deleted, in ascending order. published = 0 means nothing published yet. */
    public static List<Long> deletable(Collection<Long> versions, long published) {
        List<Long> result = new ArrayList<>();
        if (published < 1) {
            return result;
        }
        TreeSet<Long> sorted = new TreeSet<>(versions);
        Long previous = sorted.lower(published);
        for (long version : sorted) {
            if (version < published && (previous == null || version < previous)) {
                result.add(version);
            }
        }
        return result;
    }

    /** Same, reading the versions announced in "index-versions" and the published version from Hazelcast. */
    public static List<Long> deletable(HazelcastInstance hz) {
        IMap<Long, String> versions = hz.getMap(VERSIONS_MAP);
        IMap<String, Long> meta = hz.getMap(META_MAP);
        Long published = meta.get(PUBLISHED_KEY);
        return deletable(versions.keySet(), published == null ? 0 : published);
    }
}
