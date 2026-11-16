package com.skimsearch.search;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Against a fake Consul agent that records the requests it receives. */
class ConsulRegistrationTest {

    record Call(String method, String path, String body) {
    }

    final List<Call> calls = new CopyOnWriteArrayList<>();
    HttpServer fakeConsul;

    private URI startFakeConsul() throws IOException {
        fakeConsul = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        fakeConsul.createContext("/", exchange -> {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            calls.add(new Call(exchange.getRequestMethod(), exchange.getRequestURI().getPath(), body));
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        fakeConsul.start();
        return URI.create("http://127.0.0.1:" + fakeConsul.getAddress().getPort());
    }

    @AfterEach
    void stop() {
        if (fakeConsul != null) {
            fakeConsul.stop(0);
        }
    }

    @Test
    void registersWithAReadinessCheckAndDeregistersOnClose() throws Exception {
        URI consul = startFakeConsul();
        try (var reg = new ConsulRegistration(consul, "w1", "10.0.0.5", 7313, Duration.ofHours(1))) {
            assertEquals("search-w1", reg.serviceId());
        }

        assertEquals(2, calls.size());
        Call register = calls.get(0);
        assertEquals("PUT", register.method());
        assertEquals("/v1/agent/service/register", register.path());

        JsonObject service = new Gson().fromJson(register.body(), JsonObject.class);
        assertEquals("search-w1", service.get("ID").getAsString());
        assertEquals("search", service.get("Name").getAsString());
        assertEquals("10.0.0.5", service.get("Address").getAsString());
        assertEquals(7313, service.get("Port").getAsInt());
        assertEquals("w1", service.getAsJsonObject("Meta").get("workerId").getAsString());
        JsonObject check = service.getAsJsonObject("Check");
        assertEquals("http://10.0.0.5:7313/health/ready", check.get("HTTP").getAsString());
        assertEquals("1s", check.get("Interval").getAsString());

        assertEquals(new Call("PUT", "/v1/agent/service/deregister/search-w1", ""), calls.get(1));
    }

    @Test
    void keepsRetryingUntilConsulAcceptsTheRegistration() throws Exception {
        AtomicBoolean consulUp = new AtomicBoolean(false);
        fakeConsul = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        fakeConsul.createContext("/", exchange -> {
            if (consulUp.get()) {
                calls.add(new Call(exchange.getRequestMethod(), exchange.getRequestURI().getPath(), ""));
            }
            exchange.sendResponseHeaders(consulUp.get() ? 200 : 500, -1);
            exchange.close();
        });
        fakeConsul.start();
        URI consul = URI.create("http://127.0.0.1:" + fakeConsul.getAddress().getPort());

        try (var reg = new ConsulRegistration(consul, "w1", "10.0.0.5", 7313, Duration.ofMillis(200))) {
            assertTrue(calls.isEmpty(), "first attempt rejected");
            consulUp.set(true);

            long deadline = System.currentTimeMillis() + 3000;
            while (calls.isEmpty() && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertEquals("/v1/agent/service/register", calls.get(0).path(), "registered on a later attempt");
        }
    }
}