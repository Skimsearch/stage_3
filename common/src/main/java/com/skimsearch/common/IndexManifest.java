package com.skimsearch.common;

import java.util.regex.Pattern;

/**
 * Manifest of one published index version (design A). Written by Indexing as
 * index-v{N}.manifest.json next to index-v{N}.db in the shared index folder.
 *
 * Rules:
 *  - The manifest is written after the .db (temp file + atomic rename):
 *    if the manifest exists, the version is complete.
 *  - file is a bare file name, never a path: each node resolves it against its own
 *    shared folder, because the folder is mounted at different paths on each machine.
 *  - sha256 is computed on the finished .db (no -wal / -shm files).
 *    Search verifies it on its local copy before opening it.
 *  - createdAtMillis is when the version became a candidate (not when it was published).
 */
public record IndexManifest(
        long version,
        String file,
        String sha256,
        long sizeBytes,
        long createdAtMillis
) {

    private static final Pattern SHA256_HEX = Pattern.compile("[0-9a-f]{64}");

    public IndexManifest {
        if (version < 1) {
            throw new IllegalArgumentException("version must be >= 1: " + version);
        }
        if (file == null || file.isBlank() || file.contains("/") || file.contains("\\")) {
            throw new IllegalArgumentException("file must be a bare file name: " + file);
        }
        if (sha256 == null || !SHA256_HEX.matcher(sha256).matches()) {
            throw new IllegalArgumentException("sha256 must be 64 lower-case hex characters");
        }
        if (sizeBytes < 0) {
            throw new IllegalArgumentException("sizeBytes must be >= 0: " + sizeBytes);
        }
    }

    /** index-v{N}.db */
    public static String dbFileName(long version) {
        return "index-v" + version + ".db";
    }

    /** index-v{N}.manifest.json */
    public static String manifestFileName(long version) {
        return "index-v" + version + ".manifest.json";
    }
}