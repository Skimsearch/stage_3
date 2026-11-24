package com.skimsearch.search;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.hazelcast.core.HazelcastInstance;
import io.javalin.Javalin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static com.skimsearch.common.HazelcastNames.LOADED_MAP;
import static com.skimsearch.common.HazelcastNames.META_MAP;
import static com.skimsearch.common.HazelcastNames.PUBLISHED_KEY;
import static com.skimsearch.search.DesignBFixture.Posting;
import static com.skimsearch.search.DesignBFixture.book;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** The whole Search started with INDEX_DESIGN=B, answering over HTTP from Hazelcast maps. */
class SearchDesignBTest {

    @TempDir
    Path local;

    HazelcastInstance hz;
    Javalin app;
    final HttpClient http = HttpClient.newHttpClient();

    @BeforeEach
    void setUp() {
        hz = DesignBFixture.member();
        DesignBFixture.writeVersion(hz, 1,
                List.of(book(10, "Ann", "en", 2000), book(20, "Bob", "en", 2001)),
                List.of(new Posting("love", 10, 3), new Posting("love", 20, 5)));
        hz.<String, Long>getMap(META_MAP).put(PUBLISHED_KEY, 1L);
    }

    @AfterEach
    void tearDown() {
        if (app != null) {
            app.stop();
        }
        hz.shutdown();
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + app.port() + path)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void searchServesDesignBFromHazelcast() throws Exception {
        SearchConfig config = SearchConfig.from(Map.of(
                "INDEX_DESIGN", "b", "WORKER_ID", "w1", "SEARCH_PORT", "" + freePort(),
                "LOCAL_DIR", local.toString()));
        app = SearchMain.start(config, hz);

        HttpResponse<String> search = get("/search?q=love");
        assertEquals(200, search.statusCode());
        assertEquals("1", search.headers().firstValue("X-Index-Version").orElseThrow());
        JsonObject body = new Gson().fromJson(search.body(), JsonObject.class);
        assertEquals(2, body.get("total").getAsInt());
        assertEquals(20, body.getAsJsonArray("results").get(0).getAsJsonObject().get("bookId").getAsInt());

        assertEquals(200, get("/health/ready").statusCode());
        assertEquals(200, get("/books/10").statusCode());

        // announced in search-loaded, so Indexing can promote the next candidate
        long deadline = System.currentTimeMillis() + 5000;
        while (hz.getMap(LOADED_MAP).get("w1") == null && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(1L, hz.getMap(LOADED_MAP).get("w1"));
    }

    @Test
    void designBWithoutHazelcastDoesNotStart() {
        SearchConfig config = SearchConfig.from(Map.of("INDEX_DESIGN", "B", "LOCAL_DIR", local.toString()));
        assertThrows(IllegalStateException.class, () -> SearchMain.start(config, null));
    }

    @Test
    void unknownDesignIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> SearchConfig.from(Map.of("INDEX_DESIGN", "C")));
    }
}