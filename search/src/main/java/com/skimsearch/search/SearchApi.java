package com.skimsearch.search;

import com.skimsearch.common.Filters;
import com.skimsearch.common.IndexReader;
import com.skimsearch.common.IndexUnavailableException;
import io.javalin.Javalin;
import io.javalin.http.Context;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/**
 * HTTP routes of the search role. Independent of how the index is stored (design A or B):
 * it only talks to an IndexReader and a PublishedVersionSource.
 *
 * Public (through the gateway): GET /search, GET /books/{book_id}
 * Internal (Consul only):       GET /health/ready, GET /health/live
 */
public final class SearchApi {

    static final String REQUEST_ID = "X-Request-Id";
    static final String BACKEND_ID = "X-Backend-Id";
    static final String INDEX_VERSION = "X-Index-Version";

    private final String workerId;
    private final IndexReader index;
    private final PublishedVersionSource published;

    public SearchApi(String workerId, IndexReader index, PublishedVersionSource published) {
        this.workerId = workerId;
        this.index = index;
        this.published = published;
    }

    public record Readiness(boolean ready, String workerId, long servedVersion,
                            Long publishedVersion, String reason) {
    }

    /** Contract rule 5: ready only if servedVersion > 0 and >= the published version. */
    public Readiness readiness() {
        return readiness(index);
    }

    private Readiness readiness(IndexReader reader) {
        long served = reader.servedVersion();
        long pub;
        try {
            pub = published.publishedVersion();
        } catch (RuntimeException e) {
            return new Readiness(false, workerId, served, null,
                    "published version unknown: " + e.getMessage());
        }
        if (served < 1) {
            return new Readiness(false, workerId, served, pub, "no index loaded");
        }
        if (served < pub) {
            return new Readiness(false, workerId, served, pub, "index behind published version");
        }
        return new Readiness(true, workerId, served, pub, null);
    }

    public void register(Javalin app) {

        app.before(ctx -> {
            String rid = ctx.header(REQUEST_ID);
            if (rid == null || rid.isBlank()) {
                rid = UUID.randomUUID().toString();
            }
            ctx.attribute(REQUEST_ID, rid);
            ctx.header(REQUEST_ID, rid);
            ctx.header(BACKEND_ID, workerId);
        });

        app.exception(IndexUnavailableException.class, (e, ctx) ->
                ctx.status(503).json(error("index_unavailable", e.getMessage())));

        app.get("/health/live", ctx -> ctx.json(Map.of("workerId", workerId, "status", "alive")));

        app.get("/health/ready", ctx -> {
            Readiness r = readiness();
            ctx.status(r.ready() ? 200 : 503).json(r);
        });

        app.get("/books/{book_id}", this::getBook);
        app.get("/search", this::search);
    }

    /** One access-log line per request: the evidence for routing and load results. */
    public void logRequest(Context ctx, float executionMs) {
        System.out.printf(Locale.ROOT,
                "ts=%s worker=%s req=%s method=%s path=%s query=%s status=%d version=%s ms=%.3f%n",
                Instant.now(), workerId, ctx.attribute(REQUEST_ID), ctx.method(), ctx.path(),
                ctx.queryString() == null ? "-" : ctx.queryString(),
                ctx.statusCode(), ctx.res().getHeader(INDEX_VERSION), executionMs);
    }

    private void getBook(Context ctx) {
        int bookId;
        try {
            bookId = Integer.parseInt(ctx.pathParam("book_id"));
        } catch (NumberFormatException e) {
            ctx.status(400).json(error("bad_request", "book_id must be an integer"));
            return;
        }
        if (bookId <= 0) {
            ctx.status(400).json(error("bad_request", "book_id must be greater than zero"));
            return;
        }
        IndexReader snapshot = readySnapshot(ctx);
        if (snapshot == null) {
            return;
        }

        var book = snapshot.findBook(bookId);
        if (book.isEmpty()) {
            ctx.status(404).json(error("not_found", "book not found"));
            return;
        }
        ctx.json(book.get());
    }

    private void search(Context ctx) {
        String q = ctx.queryParam("q");
        if (q == null || q.isBlank()) {
            ctx.status(400).json(error("bad_request", "q parameter is required"));
            return;
        }

        List<String> tokens = SearchTokenizer.tokenize(q);
        if (tokens.isEmpty()) {
            ctx.status(400).json(error("bad_request", "q must contain a valid search term"));
            return;
        }
        if (tokens.size() > 1) {
            ctx.status(400).json(error("bad_request", "only one search term is currently supported"));
            return;
        }
        String term = tokens.get(0);

        Integer year;
        int offset;
        int limit;
        try {
            year = optionalInt(ctx, "year", null);
            offset = optionalInt(ctx, "offset", 0);
            limit = optionalInt(ctx, "limit", 20);
        } catch (IllegalArgumentException e) {
            ctx.status(400).json(error("bad_request", e.getMessage()));
            return;
        }
        if (offset < 0) {
            ctx.status(400).json(error("bad_request", "offset must be greater than or equal to zero"));
            return;
        }
        if (limit < 1 || limit > 100) {
            ctx.status(400).json(error("bad_request", "limit must be between 1 and 100"));
            return;
        }
        IndexReader snapshot = readySnapshot(ctx);
        if (snapshot == null) {
            return;
        }

        Filters filters = new Filters(ctx.queryParam("author"), ctx.queryParam("language"), year);

        // Same snapshot for both calls: total and results always come from one version.
        int total = snapshot.count(term, filters);
        var results = snapshot.search(term, filters, offset, limit);

        Map<String, Object> echoedFilters = new LinkedHashMap<>();
        if (filters.author() != null) echoedFilters.put("author", filters.author());
        if (filters.language() != null) echoedFilters.put("language", filters.language());
        if (filters.year() != null) echoedFilters.put("year", filters.year());

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("query", term);
        response.put("filters", echoedFilters);
        response.put("total", total);
        response.put("offset", offset);
        response.put("limit", limit);
        response.put("results", results);
        ctx.json(response);
    }

    /**
     * Pins one index version for this request and checks it is ready.
     * Never answers from an index older than the published one: replies 503 (and
     * returns null) so the gateway can retry on another backend (proxy_next_upstream).
     */
    private IndexReader readySnapshot(Context ctx) {
        IndexReader snapshot = index.snapshot();
        Readiness r = readiness(snapshot);
        ctx.header(INDEX_VERSION, Long.toString(r.servedVersion()));
        if (!r.ready()) {
            ctx.status(503).json(error("not_ready", r.reason()));
            return null;
        }
        return snapshot;
    }

    private static Integer optionalInt(Context ctx, String name, Integer defaultValue) {
        String raw = ctx.queryParam(name);
        if (raw == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be an integer");
        }
    }

    private static Map<String, String> error(String code, String message) {
        return Map.of("error", code, "message", message == null ? "" : message);
    }
}