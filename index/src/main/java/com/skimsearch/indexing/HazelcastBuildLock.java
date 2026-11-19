package com.skimsearch.indexing;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * A lock shared by every Indexing process of the Hazelcast cluster, built on an IMap
 * lock (available in the community edition; the CP Subsystem's FencedLock is not).
 *
 * The lock is released when its owner calls unlock(), when the owner member leaves the
 * cluster, or after LEASE_MILLIS, so a crashed indexer cannot block the others forever.
 * The lease must be longer than the longest build; if a build took longer, a second
 * indexer could enter. The owner is the calling thread of the calling member, and the
 * lock is reentrant.
 *
 * This is a best-effort lock (an IMap lock is not a linearizable CP lock): in a
 * network partition two members could hold it at the same time.
 */
public final class HazelcastBuildLock implements Lock {

    static final String MAP = "index-locks";
    static final String KEY = "build";
    static final long LEASE_MILLIS = 10 * 60_000L;

    private final IMap<String, String> map;

    private HazelcastBuildLock(IMap<String, String> map) {
        this.map = map;
    }

    public static HazelcastBuildLock of(HazelcastInstance hz) {
        return new HazelcastBuildLock(hz.getMap(MAP));
    }

    @Override
    public void lock() {
        map.lock(KEY, LEASE_MILLIS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
        lock();
    }

    @Override
    public boolean tryLock() {
        try {
            return tryLock(0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return map.tryLock(KEY, time, unit, LEASE_MILLIS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void unlock() {
        map.unlock(KEY);
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException("Conditions are not supported");
    }
}