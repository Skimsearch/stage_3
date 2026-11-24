package com.skimsearch.search;

import com.google.gson.Gson;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.skimsearch.common.Book;
import com.skimsearch.common.Filters;
import com.skimsearch.common.IndexReader;
import com.skimsearch.common.IndexUnavailableException;
import com.skimsearch.common.SearchResult;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.skimsearch.common.HazelcastNames.VERSIONS_MAP;
import static com.skimsearch.common.HazelcastNames.booksMap;
import static com.skimsearch.common.HazelcastNames.postingsMap;

/**
 * Design B: reads the index from Hazelcast maps (contract: docs/design-b-contract.md).
 *
 * - postings-v{N}: term -> int[] of interleaved bookId, tf, already in contract order.
 * - books-v{N}: bookId -> JSON of common.Book.
 * - A version is available when its manifest exists in "index-versions" (contract, rule 5).
 *
 * snapshot() pins the published version, so all calls of one request read the same maps.
 * Which versions are available is cached locally and refreshed every second, so a request
 * does not pay an extra remote call just to check it. A missing term is answered as "no
 * results" only after confirming that the version still exists: if the maps are gone (for
 * example after a full Hazelcast restart) the reader throws IndexUnavailableException
 * (HTTP 503), never an empty list.
 */
public final class HazelcastIndexReader implements IndexReader, AutoCloseable {

    private static final Gson GSON = new Gson();
    private static final int BATCH = 1_000;

    private final HazelcastInstance hz;
    private final PublishedVersionSource published;
    private final ConcurrentSkipListSet<Long> available = new ConcurrentSkipListSet<>();
    private final ScheduledExecutorService refresher;

    public HazelcastIndexReader(HazelcastInstance hz, PublishedVersionSource published, Duration refreshEvery) {
        this.hz = hz;
        this.published = published;
        refreshAvailable();
        this.refresher = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "design-b-versions");
            t.setDaemon(true);
            return t;
        });
        long ms = refreshEvery.toMillis();
        refresher.scheduleWithFixedDelay(this::refreshAvailable, ms, ms, TimeUnit.MILLISECONDS);
    }

    // ---------------------------------------------------------------- IndexReader

    @Override
    public IndexReader snapshot() {
        long version;
        try {
            version = published.publishedVersion();
        } catch (RuntimeException e) {
            return new Pinned(0);
        }
        return new Pinned(isAvailable(version) ? version : 0);
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

    /** Highest version whose manifest exists: what this Search announces as loaded (contract, rule 5). */
    public long highestAvailableVersion() {
        return available.isEmpty() ? 0 : available.last();
    }

    // ---------------------------------------------------------------- versions

    private boolean isAvailable(long version) {
        if (version < 1) {
            return false;
        }
        if (available.contains(version)) {
            return true;
        }
        // just published and not refreshed yet: ask once and remember it
        if (versionExists(version)) {
            available.add(version);
            return true;
        }
        return false;
    }

    private boolean versionExists(long version) {
        try {
            IMap<Long, String> versions = hz.getMap(VERSIONS_MAP);
            return versions.containsKey(version);
        } catch (RuntimeException e) {
            throw new IndexUnavailableException("Hazelcast unreachable", e);
        }
    }

    /** Replaces the local view with the versions that exist now (removes deleted or lost ones). */
    void refreshAvailable() {
        try {
            IMap<Long, String> versions = hz.getMap(VERSIONS_MAP);
            Set<Long> now = new HashSet<>(versions.keySet());
            available.retainAll(now);
            available.addAll(now);
        } catch (RuntimeException e) {
            log("event=design-b-versions-refresh-failed error=" + e.getClass().getSimpleName());
        }
    }

    private static void log(String event) {
        System.out.println("ts=" + Instant.now() + " " + event);
    }

    @Override
    public void close() {
        refresher.shutdownNow();
    }

    // ---------------------------------------------------------------- one pinned version

    /**
     * Reader of one version, used by one request. Keeps the last term it read, so the count and
     * the page of the same request share one read of the posting list and of the books.
     */
    private final class Pinned implements IndexReader {

        private final long version;
        private String lastKey;
        private List<int[]> lastMatches;
        private Map<Integer, Book> lastBooks;

        Pinned(long version) {
            this.version = version;
        }

        @Override
        public long servedVersion() {
            return version;
        }

        @Override
        public IndexReader snapshot() {
            return this;
        }

        @Override
        public int count(String term, Filters filters) {
            return matches(term, filters).size();
        }

        @Override
        public List<SearchResult> search(String term, Filters filters, int offset, int limit) {
            List<int[]> all = matches(term, filters);
            if (offset >= all.size()) {
                return List.of();
            }
            List<int[]> page = all.subList(offset, Math.min(all.size(), offset + limit));
            Map<Integer, Book> books = lastBooks != null ? lastBooks : books(ids(page));
            List<SearchResult> results = new ArrayList<>(page.size());
            for (int[] pair : page) {
                Book b = books.get(pair[0]);
                if (b == null) {
                    throw new IndexUnavailableException("book " + pair[0] + " missing in " + booksMap(version));
                }
                results.add(new SearchResult(b.bookId(), b.title(), b.author(), b.language(), b.year(), pair[1]));
            }
            return results;
        }

        @Override
        public Optional<Book> findBook(int bookId) {
            requireVersion();
            String json = call(() -> hz.<Integer, String>getMap(booksMap(version)).get(bookId));
            if (json == null) {
                requireStillExists();
                return Optional.empty();
            }
            return Optional.of(GSON.fromJson(json, Book.class));
        }

        /** (bookId, tf) pairs of the term that pass the filters, in contract order. */
        private List<int[]> matches(String term, Filters filters) {
            requireVersion();
            String key = term + "\u0000" + filters;
            if (key.equals(lastKey)) {
                return lastMatches;
            }
            int[] postings = call(() -> hz.<String, int[]>getMap(postingsMap(version)).get(term));
            List<int[]> pairs = new ArrayList<>();
            if (postings == null) {
                requireStillExists();   // absent term, or maps gone: never answer "0 results" for the latter
            } else {
                for (int i = 0; i + 1 < postings.length; i += 2) {
                    pairs.add(new int[]{postings[i], postings[i + 1]});
                }
            }
            Map<Integer, Book> books = null;
            if (!pairs.isEmpty() && hasFilters(filters)) {
                books = books(ids(pairs));
                List<int[]> kept = new ArrayList<>();
                for (int[] pair : pairs) {
                    Book b = books.get(pair[0]);
                    if (b == null) {
                        throw new IndexUnavailableException("book " + pair[0] + " missing in " + booksMap(version));
                    }
                    if (filters.matches(b.author(), b.language(), b.year())) {
                        kept.add(pair);
                    }
                }
                pairs = kept;
            }
            lastKey = key;
            lastMatches = pairs;
            lastBooks = books;
            return pairs;
        }

        private Map<Integer, Book> books(List<Integer> ids) {
            IMap<Integer, String> map = hz.getMap(booksMap(version));
            Map<Integer, Book> result = new java.util.HashMap<>();
            for (int from = 0; from < ids.size(); from += BATCH) {
                Set<Integer> chunk = new HashSet<>(ids.subList(from, Math.min(ids.size(), from + BATCH)));
                Map<Integer, String> json = call(() -> map.getAll(chunk));
                json.forEach((id, value) -> result.put(id, GSON.fromJson(value, Book.class)));
            }
            return result;
        }

        private void requireVersion() {
            if (version < 1) {
                throw new IndexUnavailableException("no published index version available");
            }
        }

        private void requireStillExists() {
            if (!versionExists(version)) {
                available.remove(version);
                throw new IndexUnavailableException("index version " + version + " no longer exists");
            }
        }
    }

    private static boolean hasFilters(Filters f) {
        return f.author() != null || f.language() != null || f.year() != null;
    }

    private static List<Integer> ids(List<int[]> pairs) {
        List<Integer> ids = new ArrayList<>(pairs.size());
        for (int[] pair : pairs) {
            ids.add(pair[0]);
        }
        return ids;
    }

    /** Hazelcast failures (member gone, client offline, timeout) become IndexUnavailableException. */
    private static <T> T call(java.util.function.Supplier<T> op) {
        try {
            return op.get();
        } catch (IndexUnavailableException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new IndexUnavailableException("Hazelcast read failed: " + e.getClass().getSimpleName(), e);
        }
    }
}