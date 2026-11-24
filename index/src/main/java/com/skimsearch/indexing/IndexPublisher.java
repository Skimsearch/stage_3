package com.skimsearch.indexing;

import com.google.gson.Gson;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.skimsearch.common.IndexManifest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.skimsearch.common.HazelcastNames.CANDIDATE_KEY;
import static com.skimsearch.common.HazelcastNames.LOADED_MAP;
import static com.skimsearch.common.HazelcastNames.META_MAP;
import static com.skimsearch.common.HazelcastNames.PUBLISHED_KEY;
import static com.skimsearch.common.HazelcastNames.VERSIONS_MAP;

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
 * Map and key names are defined in common (HazelcastNames), shared with the search module.
 *
 * With the shared index folder, restore() rebuilds the keys after Hazelcast restarts:
 * the candidate and index-versions from the manifests, the published version from
 * published-version.txt, which tryPromote writes whenever it raises the published version.
 */
public final class IndexPublisher {

    static final long MAX_WAIT_MILLIS = 30_000;
    static final String PUBLISHED_FILE = "published-version.txt";
    private static final Pattern MANIFEST = Pattern.compile("index-v(\\d+)\\.manifest\\.json");
    private static final Gson GSON = new Gson();

    private final IMap<String, Long> meta;
    private final IMap<Long, String> versions;
    private final IMap<String, Long> loaded;
    private final Path sharedIndexDir;
    private final LongSupplier clock;
    /** Unreadable manifests already reported, so a repeated restore() does not repeat the log line. */
    private final Set<String> reportedBad = ConcurrentHashMap.newKeySet();

    public IndexPublisher(HazelcastInstance hz) {
        this(hz, null, System::currentTimeMillis);
    }

    /** With the shared index folder, the publisher can restore its keys after a restart. */
    public IndexPublisher(HazelcastInstance hz, Path sharedIndexDir) {
        this(hz, sharedIndexDir, System::currentTimeMillis);
    }

    /** The clock is a parameter so tests can move time forward. */
    IndexPublisher(HazelcastInstance hz, LongSupplier clock) {
        this(hz, null, clock);
    }

    IndexPublisher(HazelcastInstance hz, Path sharedIndexDir, LongSupplier clock) {
        this.meta = hz.getMap(META_MAP);
        this.versions = hz.getMap(VERSIONS_MAP);
        this.loaded = hz.getMap(LOADED_MAP);
        this.sharedIndexDir = sharedIndexDir;
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
        // File first, key second: if the process dies in between, the file is ahead of what Searches saw
        // and a restart can only skip a version number. The opposite order could publish the same number
        // twice with different content.
        savePublishedFile(candidate);
        return raise(PUBLISHED_KEY, candidate);
    }

    public long publishedVersion() {
        return current(PUBLISHED_KEY);
    }

    public long candidateVersion() {
        return current(CANDIDATE_KEY);
    }

    /**
     * Call once at startup. Rebuilds index-versions and the candidate from the manifests
     * in the shared folder, and the published version from published-version.txt.
     * It only raises keys, so it is harmless if Hazelcast already had them.
     */
    public void restore() {
        if (sharedIndexDir == null || !Files.isDirectory(sharedIndexDir)) {
            return;
        }
        long highest = 0;
        try (var files = Files.list(sharedIndexDir)) {
            for (Path file : (Iterable<Path>) files::iterator) {
                Matcher m = MANIFEST.matcher(file.getFileName().toString());
                if (!m.matches()) {
                    continue;
                }
                IndexManifest manifest = readQuietly(file);
                if (manifest == null) {
                    continue;
                }
                versions.putIfAbsent(manifest.version(), GSON.toJson(manifest));
                highest = Math.max(highest, manifest.version());
            }
        } catch (IOException e) {
            throw new IllegalStateException("Cannot list " + sharedIndexDir, e);
        }
        if (highest > 0) {
            raise(CANDIDATE_KEY, highest);
        }
        long published = Math.min(readPublishedFile(), highest);
        if (published > 0) {
            raise(PUBLISHED_KEY, published);
        }
        if (highest > 0) {
            System.out.println("ts=" + Instant.now() + " event=index-keys-restored candidate="
                    + candidateVersion() + " published=" + publishedVersion());
        }
    }

    /** A corrupted or empty manifest is skipped (and reported once) instead of stopping the indexer. */
    private IndexManifest readQuietly(Path file) {
        try {
            IndexManifest manifest = IndexManifestWriter.read(file);
            if (manifest == null) {
                throw new IllegalStateException("empty manifest");
            }
            return manifest;
        } catch (RuntimeException e) {
            if (reportedBad.add(file.getFileName().toString())) {
                System.out.println("ts=" + Instant.now() + " event=manifest-skipped file="
                        + file.getFileName() + " error=" + e);
            }
            return null;
        }
    }

    private long readPublishedFile() {
        try {
            Path file = sharedIndexDir.resolve(PUBLISHED_FILE);
            return Files.exists(file) ? Long.parseLong(Files.readString(file).trim()) : 0;
        } catch (IOException | NumberFormatException e) {
            return 0;
        }
    }

    /** Atomic write; never lowers the saved version. */
    private void savePublishedFile(long version) {
        if (sharedIndexDir == null || readPublishedFile() >= version) {
            return;
        }
        Path tmp = null;
        try {
            // unique temp name: two promoters on different machines must not share one
            tmp = Files.createTempFile(sharedIndexDir, "published-version", ".tmp");
            Files.writeString(tmp, Long.toString(version));
            Files.move(tmp, sharedIndexDir.resolve(PUBLISHED_FILE), StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            System.out.println("ts=" + Instant.now() + " event=published-file-write-failed error=" + e);
            if (tmp != null) {
                try {
                    Files.deleteIfExists(tmp);
                } catch (IOException ignored) {
                    // nothing else to do
                }
            }
        }
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