package com.skimsearch.search;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.skimsearch.common.Book;
import com.skimsearch.common.Filters;
import com.skimsearch.common.IndexReader;
import com.skimsearch.common.SearchResult;
import io.javalin.Javalin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** /search must take total and results from the same index version (contract rule 7). */
class SearchSnapshotTest {

    Javalin app;

    @AfterEach
    void stop() {
        if (app != null) {
            app.stop();
        }
    }

    /** Version v has exactly v results for any term. */
    record FixedVersion(long version) implements IndexReader {
        @Override
        public List<SearchResult> search(String term, Filters filters, int offset, int limit) {
            return IntStream.rangeClosed(1, (int) version)
                    .mapToObj(i -> new SearchResult(i, "T", "A", "en", 2000, 1)).toList();
        }

        @Override
        public int count(String term, Filters filters) {
            return (int) version;
        }

        @Override
        public Optional<Book> findBook(int bookId) {
            return Optional.empty();
        }

        @Override
        public long servedVersion() {
            return version;
        }
    }

    /** Switches between v1 and v2 on every call, as if a new version arrived mid-request. */
    static final class FlippingIndex implements IndexReader {
        private long calls;

        private IndexReader current() {
            return new FixedVersion(++calls % 2 == 0 ? 2 : 1);
        }

        @Override
        public List<SearchResult> search(String t, Filters f, int o, int l) {
            return current().search(t, f, o, l);
        }

        @Override
        public int count(String t, Filters f) {
            return current().count(t, f);
        }

        @Override
        public Optional<Book> findBook(int id) {
            return current().findBook(id);
        }

        @Override
        public long servedVersion() {
            return current().servedVersion();
        }

        @Override
        public IndexReader snapshot() {
            return current();
        }
    }

    @Test
    void totalAndResultsComeFromTheSameVersion() throws Exception {
        SearchApi api = new SearchApi("w1", new FlippingIndex(), () -> 1);
        app = SearchMain.createApp(api, 8).start(0);
        HttpClient client = HttpClient.newHttpClient();

        for (int i = 0; i < 10; i++) {
            HttpResponse<String> response = client.send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + app.port() + "/search?q=love")).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode());

            JsonObject body = new Gson().fromJson(response.body(), JsonObject.class);
            int total = body.get("total").getAsInt();
            int results = body.getAsJsonArray("results").size();
            long headerVersion = Long.parseLong(response.headers().firstValue("X-Index-Version").orElseThrow());

            assertEquals(total, results, "total and results from different versions");
            assertEquals(headerVersion, total, "X-Index-Version must be the version that answered");
        }
    }
}