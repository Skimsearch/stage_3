package com.skimsearch.search;

import com.google.gson.Gson;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Registers this Search instance in the local Consul agent, as service "search",
 * with an HTTP check on /health/ready. Consul only reports the instance as passing
 * (and Consul Template only puts it in the Nginx upstream) while that check returns 200.
 *
 * - Registration is repeated periodically (idempotent), so an agent that starts later
 *   or restarts without its data still learns about this instance.
 * - On a clean shutdown the instance deregisters at once; if it crashes, the check
 *   fails within about a second and Consul drops it after DEREGISTER_AFTER.
 */
public final class ConsulRegistration implements AutoCloseable {

    public static final String SERVICE_NAME = "search";
    static final String CHECK_INTERVAL = "1s";
    static final String CHECK_TIMEOUT = "1s";
    static final String DEREGISTER_AFTER = "1m";

    private static final Gson GSON = new Gson();

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private final URI consul;
    private final String serviceId;
    private final String registrationJson;
    private final ScheduledExecutorService scheduler;
    private volatile Boolean registered;

    public ConsulRegistration(URI consul, String workerId, String advertiseHost, int port, Duration reRegisterEvery) {
        this.consul = consul;
        this.serviceId = SERVICE_NAME + "-" + workerId;
        this.registrationJson = GSON.toJson(registration(workerId, advertiseHost, port));

        register();

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "consul-registration");
            t.setDaemon(true);
            return t;
        });
        long ms = reRegisterEvery.toMillis();
        scheduler.scheduleWithFixedDelay(this::register, ms, ms, TimeUnit.MILLISECONDS);
    }

    public String serviceId() {
        return serviceId;
    }

    private Map<String, Object> registration(String workerId, String host, int port) {
        Map<String, Object> check = new LinkedHashMap<>();
        check.put("HTTP", "http://" + host + ":" + port + "/health/ready");
        check.put("Interval", CHECK_INTERVAL);
        check.put("Timeout", CHECK_TIMEOUT);
        check.put("DeregisterCriticalServiceAfter", DEREGISTER_AFTER);

        Map<String, Object> service = new LinkedHashMap<>();
        service.put("ID", serviceId);
        service.put("Name", SERVICE_NAME);
        service.put("Address", host);
        service.put("Port", port);
        service.put("Meta", Map.of("workerId", workerId));
        service.put("Check", check);
        return service;
    }

    /** PUT /v1/agent/service/register. Failures are retried on the next period. */
    void register() {
        boolean ok = put("/v1/agent/service/register", registrationJson);
        if (registered == null || registered != ok) {
            registered = ok;
            log(ok ? "event=consul-registered service=" + serviceId
                    : "event=consul-register-failed service=" + serviceId + " (retrying)");
        }
    }

    private boolean put(String path, String body) {
        try {
            HttpRequest request = HttpRequest.newBuilder(consul.resolve(path))
                    .timeout(Duration.ofSeconds(2))
                    .header("Content-Type", "application/json")
                    .PUT(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            int status = http.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
            return status == 200;
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return false;
        }
    }

    private static void log(String event) {
        System.out.println("ts=" + Instant.now() + " " + event);
    }

    /** Clean shutdown: deregister now, so the gateway stops routing here without waiting for the check. */
    @Override
    public void close() {
        scheduler.shutdownNow();
        if (put("/v1/agent/service/deregister/" + serviceId, "")) {
            log("event=consul-deregistered service=" + serviceId);
        }
    }
}