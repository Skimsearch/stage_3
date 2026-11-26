package com.skimsearch.indexing;

import com.hazelcast.core.HazelcastInstance;

import java.nio.file.Path;

/** Chooses the index builder from INDEX_DESIGN (A or B; default A). */
public final class IndexBuilders {

    private IndexBuilders() {
    }

    /** "A" or "B" from INDEX_DESIGN; null or blank means A. Fails if it is anything else. */
    public static String normalise(String design) {
        String value = design == null || design.isBlank() ? "A" : design.trim().toUpperCase(java.util.Locale.ROOT);
        if (!value.equals("A") && !value.equals("B")) {
            throw new IllegalArgumentException("INDEX_DESIGN must be A or B: " + design);
        }
        return value;
    }

    /**
     * Folder of the index of a design inside SHARED_INDEX_DIR: the folder itself for A, its subfolder
     * "design-b" for B. Design B only keeps published-version.txt there, in a folder of its own, so
     * the manifests of a design A run are never restored as B versions.
     */
    public static Path indexDir(String design, Path sharedIndexDir) {
        return normalise(design).equals("B") ? sharedIndexDir.resolve("design-b") : sharedIndexDir;
    }

    /** Fails with IllegalArgumentException if design is not A or B. */
    public static void checkDesign(String design) {
        String value = design == null || design.isBlank() ? "A" : design.trim().toUpperCase();
        if (!value.equals("A") && !value.equals("B")) {
            throw new IllegalArgumentException("INDEX_DESIGN must be A or B: " + design);
        }
    }

    /**
     * @param design          "A" (SQLite files in sharedIndexDir) or "B" (Hazelcast maps); null or blank means A
     * @param sharedIndexDir  design A: where versions are written; design B: where published-version.txt lives.
     *                        Use a different folder for each design, so that the manifests of one do not
     *                        reach the other.
     */
    public static IndexBuilder create(String design, DatalakeReader datalake, Path sharedIndexDir,
                                      HazelcastInstance hz) {
        String value = design == null || design.isBlank() ? "A" : design.trim().toUpperCase();
        return switch (value) {
            case "A" -> new VersionedIndexingService(datalake, new IndexVersionWriter(sharedIndexDir));
            case "B" -> new HazelcastIndexingService(datalake, hz, sharedIndexDir);
            default -> throw new IllegalArgumentException("INDEX_DESIGN must be A or B: " + design);
        };
    }
}
