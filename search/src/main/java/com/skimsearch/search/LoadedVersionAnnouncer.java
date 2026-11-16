package com.skimsearch.search;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Announces this Search's highest loaded index version in the IMap "search-loaded"
 * (workerId -> version), so Indexing can promote a candidate once enough Searches have it.
 *
 * - Each entry has a TTL and is renewed periodically: if the node dies, its entry
 *   expires by itself and never blocks a promotion.
 * - On a clean shutdown the entry is removed at once.
 * - Every Search announces, even with version 0, because the entries also tell
 *   Indexing how many Searches are active.
 */
public final class LoadedVersionAnnouncer implements AutoCloseable {

    public static final String LOADED_MAP = "search-loaded";

    private final HazelcastInstance hz;
    private final String workerId;
    private final LongSupplier highestLoaded;
    private final Duration ttl;
    private final ScheduledExecutorService scheduler;
    private volatile IMap<String, Long> loaded;
    private volatile long lastAnnounced = -1;
    private volatile Boolean reachable;

    public LoadedVersionAnnouncer(HazelcastInstance hz, String workerId, LongSupplier highestLoaded,
                                  Duration every, Duration ttl) {
        this.hz = hz;
        this.workerId = workerId;
        this.highestLoaded = highestLoaded;
        this.ttl = ttl;

        announce();

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "loaded-version-announcer");
            t.setDaemon(true);
            return t;
        });
        long ms = every.toMillis();
        scheduler.scheduleWithFixedDelay(this::announce, ms, ms, TimeUnit.MILLISECONDS);
    }

    /** Puts (or renews) this worker's entry with a fresh TTL. */
    void announce() {
        long version = highestLoaded.getAsLong();
        try {
            if (loaded == null) {
                loaded = hz.getMap(LOADED_MAP);
            }
            loaded.put(workerId, version, ttl.toMillis(), TimeUnit.MILLISECONDS);
            setReachable(true);
            if (version != lastAnnounced) {
                log("event=loaded-version-announced version=" + version);
                lastAnnounced = version;
            }
        } catch (RuntimeException e) {
            setReachable(false);
        }
    }

    private void setReachable(boolean now) {
        if (reachable == null || reachable != now) {
            reachable = now;
            if (!now) {
                log("event=loaded-version-announce-failed (Hazelcast unreachable, retrying)");
            }
        }
    }

    private static void log(String event) {
        System.out.println("ts=" + Instant.now() + " " + event);
    }

    /** Clean shutdown: stop renewing and remove the entry so promotion does not wait for the TTL. */
    @Override
    public void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        try {
            if (loaded != null) {
                loaded.remove(workerId);
                log("event=loaded-version-withdrawn");
            }
        } catch (RuntimeException ignored) {
            // Hazelcast gone: the TTL removes the entry
        }
    }
}