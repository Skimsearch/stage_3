package com.skimsearch.indexing;

import com.google.gson.Gson;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.skimsearch.common.Book;
import com.skimsearch.common.HazelcastNames;
import com.skimsearch.common.IndexManifest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.skimsearch.common.HazelcastNames.CANDIDATE_KEY;
import static com.skimsearch.common.HazelcastNames.META_MAP;
import static com.skimsearch.common.HazelcastNames.PUBLISHED_KEY;
import static com.skimsearch.common.HazelcastNames.VERSIONS_MAP;
import static com.skimsearch.common.HazelcastNames.booksMap;
import static com.skimsearch.common.HazelcastNames.postingsMap;

/**
 * Design B: builds the next index version in Hazelcast memory (contract: docs/design-b-contract.md).
 *
 * Version N is two maps, postings-v{N} (term -> int[] of interleaved bookId, tf, sorted by tf DESC then
 * bookId ASC) and books-v{N} (bookId -> JSON of common.Book). It starts as a copy of the latest version that
 * has a manifest in "index-versions", plus the given books (re-indexing a book replaces it). Maps of a
 * version are never written after the manifest is announced.
 *
 * The number of the new version is above every number ever seen: the candidate, the published version,
 * the versions in "index-versions" and published-version.txt, so a rebuild after a full restart of
 * Hazelcast never reuses a number (contract, rule 8). If Hazelcast has lost every version while some version
 * number is already known (a restart of the cluster while this process keeps running), the previous maps are
 * gone, so the new version is rebuilt from EVERY complete book of the datalake plus the given ones. Without
 * this, the new version would hold only the given books and the rest would vanish from the index.
 *
 * The caller must hold the build lock (IndexingRole does).
 */
public final class HazelcastIndexingService implements IndexBuilder {

    private static final int BATCH = 1_000;
    private static final Gson GSON = new Gson();

    private final DatalakeReader datalake;
    private final HazelcastInstance hz;
    private final Path sharedIndexDir;
    // written and read under the build lock held by IndexingRole
    private volatile int lastBuildBooks = -1;

    /** sharedIndexDir only holds published-version.txt; it may be null (then the file is ignored). */
    public HazelcastIndexingService(DatalakeReader datalake, HazelcastInstance hz, Path sharedIndexDir) {
        this.datalake = datalake;
        this.hz = hz;
        this.sharedIndexDir = sharedIndexDir;
    }

    @Override
    public IndexManifest indexBooks(List<Integer> bookIds) {
        if (bookIds.isEmpty()) {
            throw new IllegalArgumentException("No book IDs provided: a version must contain at least one book");
        }
        long started = System.nanoTime();

        IMap<Long, String> versions = hz.getMap(VERSIONS_MAP);
        if (versions.isEmpty() && highestKnownVersion() > 0) {
            // Hazelcast lost every version: rebuild from every complete book, not only the requested ones
            Set<Integer> all = new LinkedHashSet<>(datalake.completeBookIds());
            all.addAll(bookIds);
            bookIds = new ArrayList<>(all);
            log("event=index-rebuild-from-datalake books=" + bookIds.size());
        }

        // Read and tokenise everything first: a missing book fails before any map is touched.
        List<IndexStore.BookIndexData> books = new ArrayList<>();
        for (int bookId : bookIds) {
            DatalakeReader.RawBook raw = datalake.read(bookId);
            books.add(new IndexStore.BookIndexData(
                    MetadataExtractor.extract(bookId, raw.header()),
                    Tokenizer.termFrequencies(raw.body())));
        }

        long previous = 0;
        for (Long version : versions.keySet()) {
            previous = Math.max(previous, version);
        }
        long next = Math.max(previous, highestKnownVersion()) + 1;

        IMap<String, int[]> postings = hz.getMap(postingsMap(next));
        IMap<Integer, String> bookMap = hz.getMap(booksMap(next));
        // leftovers of a build that failed before its manifest was announced
        postings.clear();
        bookMap.clear();

        Set<Integer> replaced = new HashSet<>();
        Map<String, List<int[]>> added = new HashMap<>();
        for (IndexStore.BookIndexData book : books) {
            int bookId = book.metadata().bookId();
            replaced.add(bookId);
            for (Map.Entry<String, Integer> term : book.termFrequencies().entrySet()) {
                added.computeIfAbsent(term.getKey(), k -> new ArrayList<>())
                        .add(new int[] {bookId, term.getValue()});
            }
        }

        Set<String> untouched = new HashSet<>(added.keySet());
        if (previous > 0) {
            IMap<String, int[]> oldPostings = hz.getMap(postingsMap(previous));
            IMap<Integer, String> oldBooks = hz.getMap(booksMap(previous));
            if (oldBooks.isEmpty()) {
                throw new IllegalStateException("The maps of version " + previous
                        + " are missing, cannot build version " + next + " from it");
            }
            for (List<String> keys : chunks(new ArrayList<>(oldPostings.keySet()))) {
                Map<String, int[]> out = new HashMap<>();
                for (Map.Entry<String, int[]> old : oldPostings.getAll(new HashSet<>(keys)).entrySet()) {
                    untouched.remove(old.getKey());
                    int[] merged = mergePostings(old.getValue(), replaced, added.get(old.getKey()));
                    if (merged.length > 0) {
                        out.put(old.getKey(), merged);
                    }
                }
                postings.putAll(out);
            }
            for (List<Integer> ids : chunks(new ArrayList<>(oldBooks.keySet()))) {
                Map<Integer, String> out = new HashMap<>(oldBooks.getAll(new HashSet<>(ids)));
                out.keySet().removeAll(replaced);
                bookMap.putAll(out);
            }
        }
        Map<String, int[]> newTerms = new HashMap<>();
        for (String term : untouched) {
            newTerms.put(term, mergePostings(new int[0], replaced, added.get(term)));
        }
        postings.putAll(newTerms);

        String indexedAt = Instant.now().toString();
        Map<Integer, String> newBooks = new HashMap<>();
        for (IndexStore.BookIndexData book : books) {
            BookMetadata m = book.metadata();
            newBooks.put(m.bookId(), GSON.toJson(new Book(m.bookId(), m.title(), m.author(), m.language(),
                    m.year(), book.termFrequencies().size(), indexedAt)));
        }
        bookMap.putAll(newBooks);

        lastBuildBooks = books.size();
        IndexDigest digest = digestOf(postings, bookMap);
        IndexManifest manifest = new IndexManifest(next, "postings-v" + next, digest.sha256(),
                digest.sizeBytes(), System.currentTimeMillis());
        log("event=index-version-built version=" + next + " books=" + bookMap.size()
                + " terms=" + postings.size() + " bytes=" + digest.sizeBytes()
                + " ms=" + (System.nanoTime() - started) / 1_000_000);
        return manifest;
    }

    @Override
    public int booksInLastBuild(int requested) {
        return lastBuildBooks >= 0 ? lastBuildBooks : requested;
    }

    /**
     * Previous list of a term without the replaced books, plus the new entries, in the contract order:
     * tf DESC, then bookId ASC. Both lists are interleaved bookId, tf pairs.
     */
    static int[] mergePostings(int[] old, Set<Integer> replaced, List<int[]> added) {
        List<int[]> pairs = new ArrayList<>();
        for (int i = 0; i < old.length; i += 2) {
            if (!replaced.contains(old[i])) {
                pairs.add(new int[] {old[i], old[i + 1]});
            }
        }
        if (added != null) {
            pairs.addAll(added);
        }
        pairs.sort((a, b) -> a[1] != b[1] ? Integer.compare(b[1], a[1]) : Integer.compare(a[0], b[0]));
        int[] result = new int[pairs.size() * 2];
        for (int i = 0; i < pairs.size(); i++) {
            result[2 * i] = pairs.get(i)[0];
            result[2 * i + 1] = pairs.get(i)[1];
        }
        return result;
    }

    /** Reads the finished maps back in canonical order (contract, rule 4). */
    private static IndexDigest digestOf(IMap<String, int[]> postings, IMap<Integer, String> books) {
        IndexDigest digest = new IndexDigest();
        List<String> terms = new ArrayList<>(postings.keySet());
        Collections.sort(terms);
        for (List<String> keys : chunks(terms)) {
            Map<String, int[]> values = postings.getAll(new HashSet<>(keys));
            for (String term : keys) {
                digest.addPostings(term, values.get(term));
            }
        }
        List<Integer> ids = new ArrayList<>(books.keySet());
        Collections.sort(ids);
        for (List<Integer> chunk : chunks(ids)) {
            Map<Integer, String> values = books.getAll(new HashSet<>(chunk));
            for (int id : chunk) {
                digest.addBook(id, values.get(id));
            }
        }
        return digest;
    }

    /** Highest version number ever seen: candidate, published, and published-version.txt. */
    private long highestKnownVersion() {
        IMap<String, Long> meta = hz.getMap(META_MAP);
        long highest = 0;
        for (String key : List.of(CANDIDATE_KEY, PUBLISHED_KEY)) {
            Long value = meta.get(key);
            if (value != null) {
                highest = Math.max(highest, value);
            }
        }
        return Math.max(highest, readPublishedFile());
    }

    private long readPublishedFile() {
        if (sharedIndexDir == null) {
            return 0;
        }
        try {
            Path file = sharedIndexDir.resolve(IndexPublisher.PUBLISHED_FILE);
            return Files.exists(file) ? Long.parseLong(Files.readString(file).trim()) : 0;
        } catch (IOException | NumberFormatException e) {
            return 0;
        }
    }

    private static <T> List<List<T>> chunks(List<T> items) {
        List<List<T>> result = new ArrayList<>();
        for (int from = 0; from < items.size(); from += BATCH) {
            result.add(items.subList(from, Math.min(items.size(), from + BATCH)));
        }
        return result;
    }

    private static void log(String event) {
        System.out.println("ts=" + Instant.now() + " " + event);
    }
}