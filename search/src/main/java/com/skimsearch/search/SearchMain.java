package com.skimsearch.search;

import com.google.gson.Gson;
import io.javalin.Javalin;
import io.javalin.json.JsonMapper;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import java.lang.reflect.Type;

/**
 * Standalone launcher of the search role (until Ines's single worker program calls SearchMain.start).
 */
public final class SearchMain {

    private SearchMain() {
    }

    public static void main(String[] args) {
        start(SearchConfig.fromEnvironment());
    }

    public static Javalin start(SearchConfig config) {

        SqliteIndexReader index = new SqliteIndexReader(config.indexPath(), config.indexVersion());
        PublishedVersionSource published = config::publishedVersion; // fake until Hazelcast

        SearchApi api = new SearchApi(config.workerId(), index, published);

        Gson gson = new Gson();
        JsonMapper mapper = new JsonMapper() {
            @Override
            public String toJsonString(Object obj, Type type) {
                return gson.toJson(obj, type);
            }

            @Override
            public <T> T fromJsonString(String json, Type targetType) {
                return gson.fromJson(json, targetType);
            }
        };

        Javalin app = Javalin.create(cfg -> {
            cfg.http.defaultContentType = "application/json";
            cfg.jsonMapper(mapper);
            cfg.jetty.threadPool = new QueuedThreadPool(config.threads());
            cfg.requestLogger.http(api::logRequest);
        });

        api.register(app);
        app.start(config.port());

        System.out.printf("search role started: worker=%s port=%d threads=%d index=%s version=%d%n",
                config.workerId(), config.port(), config.threads(),
                config.indexPath().toAbsolutePath(), config.indexVersion());
        return app;
    }
}