package com.skimsearch.indexing;

import com.skimsearch.common.IndexManifest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Builds a new immutable index version (index-v{N}.db) from the previous one plus new books.
 * The file is written under a temporary name and renamed only when it is complete and
 * self-contained (no -wal / -shm files), so a half-built version is never visible.
 */
public final class IndexVersionWriter {

    private static final Pattern DB_FILE = Pattern.compile("index-v(\\d+)\\.db");

    private final Path indexDir;

    public IndexVersionWriter(Path indexDir) {
        this.indexDir = indexDir.toAbsolutePath();
    }

    /** Highest N such that index-vN.db exists, or 0 if there is none. */
    public long latestVersion() {
        if (!Files.isDirectory(indexDir)) {
            return 0;
        }
        try (Stream<Path> files = Files.list(indexDir)) {
            return files.map(p -> DB_FILE.matcher(p.getFileName().toString()))
                    .filter(Matcher::matches)
                    .mapToLong(m -> Long.parseLong(m.group(1)))
                    .max()
                    .orElse(0);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot list " + indexDir, e);
        }
    }

    /** Builds version latest+1 = previous version + the given books. Returns the final file. */
    public Path buildNextVersion(List<IndexStore.BookIndexData> books) {
        long previous = latestVersion();
        long next = previous + 1;
        Path tmp = indexDir.resolve(IndexManifest.dbFileName(next) + ".tmp");
        Path target = indexDir.resolve(IndexManifest.dbFileName(next));
        try {
            Files.createDirectories(indexDir);
            Files.deleteIfExists(tmp);
            if (previous > 0) {
                Files.copy(indexDir.resolve(IndexManifest.dbFileName(previous)), tmp);
            }
            IndexStore store = new IndexStore(tmp);
            for (IndexStore.BookIndexData book : books) {
                store.indexBook(book.metadata(), book.termFrequencies());
            }
            makeSelfContained(tmp);
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE);
            return target;
        } catch (IOException e) {
            throw new IllegalStateException("Cannot build index version " + next, e);
        }
    }

    /** Moves everything from the -wal file into the main file and leaves no side files. */
    static void makeSelfContained(Path db) {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db);
             Statement st = c.createStatement()) {
            st.execute("PRAGMA wal_checkpoint(TRUNCATE)");
            st.execute("PRAGMA journal_mode = DELETE");
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot finalise " + db, e);
        }
        if (Files.exists(Path.of(db + "-wal")) || Files.exists(Path.of(db + "-shm"))) {
            throw new IllegalStateException("Side files left next to " + db);
        }
    }
}