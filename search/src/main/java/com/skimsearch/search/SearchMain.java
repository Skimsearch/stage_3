package com.skimsearch.search;

import com.google.gson.Gson;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.skimsearch.common.IndexReader;
import io.javalin.Javalin;
import io.javalin.json.JsonMapper;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import java.lang.reflect.Type;
import java.time.Duration;

/**
 * Standalone launcher of the search role (until Ines's single worker program calls SearchMain.start).
 */
public final class SearchMain {

    private static final Duration POLL_EVERY = Duration.ofSeconds(1);
    private static final Duration REFRESH_EVERY = Duration.ofSeconds(1);

    private SearchMain() {
    }

    public static void main(String[] args) {
        SearchConfig config = SearchConfig.fromEnvironment();
        PublishedVersionSource published = config.usesHazelcast()
                ? new HazelcastPublishedVersion(newClient(config), config.lastPublishedFile(), POLL_EVERY)
                : config::publishedVersion; // local tests without Hazelcast
        start(config, published);
    }

    public static Javalin start(SearchConfig config, PublishedVersionSource published) {

        IndexReader index = config.usesVersionedIndex()
                ? new VersionedIndex(config.sharedIndexDir(), config.localIndexDir(), published, REFRESH_EVERY)
                : new SqliteIndexReader(config.indexPath(), config.indexVersion());
        SearchApi api = new SearchApi(config.workerId(), index, published);

        Javalin app = createApp(api, config.threads());
        app.start(config.port());

        System.out.printf("search role started: worker=%s port=%d threads=%d index=%s hazelcast=%s%n",
                config.workerId(), config.port(), config.threads(),
                config.usesVersionedIndex()
                        ? "versioned " + config.sharedIndexDir().toAbsolutePath()
                        : config.indexPath().toAbsolutePath() + " v" + config.indexVersion(),
                config.usesHazelcast() ? config.hzAddresses() : "off");
        return app;
    }

    /** Javalin with Gson, the thread pool and the access log; routes registered, not started. */
    static Javalin createApp(SearchApi api, int threads) {
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
            cfg.jetty.threadPool = new QueuedThreadPool(threads);
            cfg.requestLogger.http(api::logRequest);
        });

        api.register(app);
        return app;
    }

    /**
     * Hazelcast client that starts even if the cluster is down (async start) and keeps
     * reconnecting every 0.5-2 s, so a recovered cluster is noticed quickly.
     */
    static HazelcastInstance newClient(SearchConfig config) {
        ClientConfig cc = new ClientConfig();
        cc.setClusterName(config.hzClusterName());
        cc.setInstanceName(config.workerId() + "-search");
        config.hzAddresses().forEach(a -> cc.getNetworkConfig().addAddress(a));

        ClientConnectionStrategyConfig strategy = cc.getConnectionStrategyConfig();
        strategy.setAsyncStart(true);
        strategy.setReconnectMode(ClientConnectionStrategyConfig.ReconnectMode.ASYNC);
        strategy.getConnectionRetryConfig()
                .setClusterConnectTimeoutMillis(-1)
                .setInitialBackoffMillis(500)
                .setMaxBackoffMillis(2000);

        return HazelcastClient.newHazelcastClient(cc);
    }
}