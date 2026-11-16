package com.skimsearch.search;

import com.google.gson.Gson;
import com.skimsearch.common.Book;
import com.skimsearch.common.Filters;
import com.skimsearch.common.IndexManifest;
import com.skimsearch.common.IndexReader;
import com.skimsearch.common.IndexUnavailableException;
import com.skimsearch.common.SearchResult;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Design A: this Search node keeps its own local copies of published index versions.
 *
 * - Reads index-v{N}.manifest.json files from the shared index folder (a version
 *   without manifest is not complete and is ignored).
 * - Loads at most two versions: the published one (to serve it) and the highest
 *   one available (the candidate), so a new version is ready before it is published.
 * - Each .db is copied to local disk and verified there (size + SHA-256), so a bad
 *   network copy is detected too. A version that fails is not loaded and is retried.
 * - Serves the highest loaded version that is not above the published one.
 * - Deletes local versions older than the one before the served one (that previous
 *   version is kept so requests still running on it can finish).
 */
public final class VersionedIndex implements IndexReader, AutoCloseable {

    private static final Pattern MANIFEST = Pattern.compile("index-v(\\d+)\\.manifest\\.json");
    private static final Gson GSON = new Gson();

    private final Path sharedDir;
    private final Path localDir;
    private final PublishedVersionSource published;
    private final ConcurrentSkipListMap<Long, SqliteIndexReader> loaded = new ConcurrentSkipListMap<>();
    private final ScheduledExecutorService refresher;
    private volatile long lastLoggedServed;

    public VersionedIndex(Path sharedDir, Path localDir, PublishedVersionSource published, Duration refreshEvery) {
        this.sharedDir = sharedDir.toAbsolutePath();
        this.localDir = localDir.toAbsolutePath();
        this.published = published;

        refresh();

        this.refresher = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "versioned-index-refresh");
            t.setDaemon(true);
            return t;
        });
        long ms = refreshEvery.toMillis();
        refresher.scheduleWithFixedDelay(this::refreshQuietly, ms, ms, TimeUnit.MILLISECONDS);
    }

    // ---------------------------------------------------------------- IndexReader

    @Override
    public IndexReader snapshot() {
        Map.Entry<Long, SqliteIndexReader> entry = servedEntry();
        return entry == null ? NOT_LOADED : entry.getValue();
    }

    @Override
    public long servedVersion() {
        return snapshot().servedVersion();
    }

    @Override
    public List<SearchResult> search(String term, Filters filters, int offset, int limit) {
        return snapshot().search(term, filters, offset, limit);
    }

    @Override
    public int count(String term, Filters filters) {
        return snapshot().count(term, filters);
    }

    @Override
    public Optional<Book> findBook(int bookId) {
        return snapshot().findBook(bookId);
    }

    /** Highest version loaded and verified on this node (the one announced to Indexing). */
    public long highestLoadedVersion() {
        return loaded.isEmpty() ? 0 : loaded.lastKey();
    }

    // ---------------------------------------------------------------- refresh

    /** One refresh cycle: load the published and the candidate version, then clean up. */
    void refresh() {
        TreeMap<Long, IndexManifest> manifests = readManifests();
        Long pub = publishedOrNull();

        if (pub != null && manifests.containsKey(pub)) {
            ensureLoaded(manifests.get(pub));
        }
        if (!manifests.isEmpty()) {
            ensureLoaded(manifests.lastEntry().getValue());
        }

        logServedChange();
        cleanUp();
    }

    private void refreshQuietly() {
        try {
            refresh();
        } catch (RuntimeException e) {
            log("event=index-refresh-failed error=" + e);
        }
    }

    /** Highest loaded version <= published. If the published one is unknown, nothing is served. */
    private Map.Entry<Long, SqliteIndexReader> servedEntry() {
        Long pub = publishedOrNull();
        return pub == null ? null : loaded.floorEntry(pub);
    }

    private Long publishedOrNull() {
        try {
            return published.publishedVersion();
        } catch (RuntimeException e) {
            return null;
        }
    }

    private TreeMap<Long, IndexManifest> readManifests() {
        TreeMap<Long, IndexManifest> manifests = new TreeMap<>();
        if (!Files.isDirectory(sharedDir)) {
            return manifests;
        }
        try (Stream<Path> files = Files.list(sharedDir)) {
            for (Path file : files.toList()) {
                Matcher m = MANIFEST.matcher(file.getFileName().toString());
                if (m.matches()) {
                    readManifest(file, Long.parseLong(m.group(1))).ifPresent(man -> manifests.put(man.version(), man));
                }
            }
        } catch (IOException e) {
            log("event=shared-index-unreadable dir=" + sharedDir + " error=" + e);
        }
        return manifests;
    }

    private Optional<IndexManifest> readManifest(Path file, long versionInName) {
        try {
            IndexManifest man = GSON.fromJson(Files.readString(file), IndexManifest.class);
            if (man.version() != versionInName || !man.file().equals(IndexManifest.dbFileName(man.version()))) {
                log("event=manifest-rejected file=" + file.getFileName() + " reason=name-mismatch");
                return Optional.empty();
            }
            return Optional.of(man);
        } catch (IOException | RuntimeException e) {
            log("event=manifest-rejected file=" + file.getFileName() + " reason=" + e.getClass().getSimpleName());
            return Optional.empty();
        }
    }

    /** Copies the version to local disk, verifies it and opens it. Does nothing if already loaded. */
    private void ensureLoaded(IndexManifest man) {
        if (loaded.containsKey(man.version())) {
            return;
        }
        long start = System.nanoTime();
        Path local = localDir.resolve(man.file());
        try {
            Files.createDirectories(localDir);
            boolean reused = Files.isRegularFile(local) && matches(local, man);
            if (!reused) {
                Path part = localDir.resolve(man.file() + ".part");
                Files.copy(sharedDir.resolve(man.file()), part, StandardCopyOption.REPLACE_EXISTING);
                if (!matches(part, man)) {
                    Files.deleteIfExists(part);
                    log("event=index-rejected version=" + man.version() + " reason=checksum-mismatch");
                    return;
                }
                Files.move(part, local, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            }
            loaded.put(man.version(), new SqliteIndexReader(local, man.version()));
            log(String.format(java.util.Locale.ROOT,
                    "event=index-loaded version=%d bytes=%d reused=%s ms=%.1f created_at=%d",
                    man.version(), man.sizeBytes(), reused, (System.nanoTime() - start) / 1e6,
                    man.createdAtMillis()));
        } catch (IOException e) {
            log("event=index-load-failed version=" + man.version() + " error=" + e);
        }
    }

    private static boolean matches(Path file, IndexManifest man) throws IOException {
        return Files.size(file) == man.sizeBytes() && sha256(file).equals(man.sha256());
    }

    private void logServedChange() {
        long served = servedVersion();
        if (served != lastLoggedServed) {
            log("event=served-version-changed from=" + lastLoggedServed + " to=" + served);
            lastLoggedServed = served;
        }
    }

    /** Keeps the served version, the one before it and anything newer; deletes the rest. */
    private void cleanUp() {
        Map.Entry<Long, SqliteIndexReader> served = servedEntry();
        if (served == null) {
            return;
        }
        Long previous = loaded.lowerKey(served.getKey());
        if (previous == null) {
            return;
        }
        for (Long old : List.copyOf(loaded.headMap(previous).keySet())) {
            loaded.remove(old);
            Path file = localDir.resolve(IndexManifest.dbFileName(old));
            try {
                Files.deleteIfExists(file);
                log("event=index-deleted version=" + old);
            } catch (IOException e) {
                // e.g. still open on Windows: the file is removed on a later cycle or restart
                log("event=index-delete-postponed version=" + old);
            }
        }
    }

    static String sha256(Path file) throws IOException {
        try (InputStream in = Files.newInputStream(file)) {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[64 * 1024];
            int n;
            while ((n = in.read(buffer)) > 0) {
                digest.update(buffer, 0, n);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void log(String event) {
        System.out.println("ts=" + Instant.now() + " " + event);
    }

    @Override
    public void close() {
        refresher.shutdownNow();
    }

    /** Served when no version can be served yet: version 0, every query fails with 503. */
    private static final IndexReader NOT_LOADED = new IndexReader() {
        @Override
        public List<SearchResult> search(String term, Filters filters, int offset, int limit) {
            throw new IndexUnavailableException("no index version loaded");
        }

        @Override
        public int count(String term, Filters filters) {
            throw new IndexUnavailableException("no index version loaded");
        }

        @Override
        public Optional<Book> findBook(int bookId) {
            throw new IndexUnavailableException("no index version loaded");
        }

        @Override
        public long servedVersion() {
            return 0;
        }
    };
}