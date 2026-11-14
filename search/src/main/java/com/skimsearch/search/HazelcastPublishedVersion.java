package com.skimsearch.search;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.skimsearch.common.IndexUnavailableException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Published index version read from Hazelcast (IMap "index-meta", key "index.publishedVersion").
 *
 * - /search never calls Hazelcast: it reads a local variable.
 * - The variable is updated by an entry listener (fast) and by polling (backup,
 *   because events can be lost during partition migration or a reconnection).
 * - It only goes up: a late or repeated event cannot move it back.
 * - The last known value is saved in a node-local file, so a Search restarted while
 *   Hazelcast is unreachable still knows it. Without Hazelcast nobody can publish,
 *   so that value is still valid (limit: a network partition, declared in the report).
 *   Delete that file when resetting an experiment.
 */
public final class HazelcastPublishedVersion implements PublishedVersionSource, AutoCloseable {

    public static final String META_MAP = "index-meta";
    public static final String PUBLISHED_KEY = "index.publishedVersion";

    private static final long UNKNOWN = -1;

    private final HazelcastInstance hz;
    private final Path lastKnownFile;
    private final AtomicLong current = new AtomicLong(UNKNOWN);
    private final ScheduledExecutorService poller;
    private volatile IMap<String, Long> meta;
    private volatile UUID listenerId;
    private volatile Boolean reachable;

    public HazelcastPublishedVersion(HazelcastInstance hz, Path lastKnownFile, Duration pollEvery) {
        this.hz = hz;
        this.lastKnownFile = lastKnownFile;

        readLastKnownFile();
        poll();

        this.poller = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "published-version-poller");
            t.setDaemon(true);
            return t;
        });
        long ms = pollEvery.toMillis();
        poller.scheduleWithFixedDelay(this::poll, ms, ms, TimeUnit.MILLISECONDS);
    }

    @Override
    public long publishedVersion() {
        long v = current.get();
        if (v == UNKNOWN) {
            throw new IndexUnavailableException("Hazelcast not reached yet and no saved value");
        }
        return v;
    }

    /**
     * Reads Hazelcast once. If it is unreachable or the key is missing, keeps the current value.
     * The map proxy and the listener are created here, not in the constructor, because a client
     * that started while the cluster was down cannot create them yet.
     */
    void poll() {
        try {
            if (meta == null) {
                meta = hz.getMap(META_MAP);
            }
            if (listenerId == null) {
                listenerId = meta.addEntryListener(new Listener(), PUBLISHED_KEY, true);
            }
            Long v = meta.get(PUBLISHED_KEY);
            if (v != null) {
                update(v);
            }
            setReachable(true, null);
        } catch (RuntimeException e) {
            setReachable(false, e);
        }
    }

    private void update(long v) {
        long before = current.getAndAccumulate(v, Math::max);
        if (v > before) {
            System.out.printf("ts=%s event=published-version-changed from=%d to=%d%n",
                    Instant.now(), before, v);
            writeLastKnownFile();
        }
    }

    /** Logs only when reachability changes, not on every poll. */
    private void setReachable(boolean now, RuntimeException error) {
        if (reachable == null || reachable != now) {
            reachable = now;
            System.out.printf("ts=%s event=hazelcast-%s published=%d%s%n",
                    Instant.now(), now ? "reachable" : "unreachable", current.get(),
                    error == null ? "" : " error=" + error.getClass().getSimpleName());
        }
    }

    private final class Listener
            implements EntryAddedListener<String, Long>, EntryUpdatedListener<String, Long> {

        @Override
        public void entryAdded(EntryEvent<String, Long> event) {
            update(event.getValue());
        }

        @Override
        public void entryUpdated(EntryEvent<String, Long> event) {
            update(event.getValue());
        }
    }

    private void readLastKnownFile() {
        try {
            if (Files.isRegularFile(lastKnownFile)) {
                current.set(Long.parseLong(Files.readString(lastKnownFile).trim()));
            }
        } catch (IOException | NumberFormatException e) {
            System.out.println("ignoring unreadable " + lastKnownFile + ": " + e);
        }
    }

    /** Write to a temp file and rename, so a crash never leaves a half-written number. */
    private synchronized void writeLastKnownFile() {
        try {
            Path dir = lastKnownFile.toAbsolutePath().getParent();
            Files.createDirectories(dir);
            Path tmp = Files.createTempFile(dir, "published", ".tmp");
            Files.writeString(tmp, Long.toString(current.get()));
            Files.move(tmp, lastKnownFile,
                    StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            System.out.println("could not save last known published version: " + e);
        }
    }

    @Override
    public void close() {
        poller.shutdownNow();
        UUID id = listenerId;
        if (id != null && meta != null) {
            try {
                meta.removeEntryListener(id);
            } catch (RuntimeException ignored) {
                // Hazelcast already gone
            }
        }
    }
}