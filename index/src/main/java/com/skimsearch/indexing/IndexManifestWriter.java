package com.skimsearch.indexing;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.skimsearch.common.IndexManifest;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Writes index-v{N}.manifest.json next to a finished index-v{N}.db.
 * The manifest is written last, with a temporary file and an atomic rename:
 * if the manifest exists, the version is complete.
 */
public final class IndexManifestWriter {

    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

    private IndexManifestWriter() {
    }

    public static IndexManifest write(Path db, long version) {
        Path dir = db.toAbsolutePath().getParent();
        Path target = dir.resolve(IndexManifest.manifestFileName(version));
        if (Files.exists(target)) {
            throw new IllegalStateException("Manifest already exists (versions are immutable): " + target);
        }
        try {
            IndexManifest manifest = new IndexManifest(
                    version,
                    db.getFileName().toString(),
                    sha256(db),
                    Files.size(db),
                    System.currentTimeMillis());
            Path tmp = dir.resolve(target.getFileName() + ".tmp");
            Files.writeString(tmp, GSON.toJson(manifest));
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE);
            return manifest;
        } catch (IOException e) {
            throw new IllegalStateException("Cannot write manifest " + target, e);
        }
    }

    public static IndexManifest read(Path manifestFile) {
        try {
            return GSON.fromJson(Files.readString(manifestFile), IndexManifest.class);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read manifest " + manifestFile, e);
        }
    }

    /** SHA-256 of the file, 64 lower-case hex characters (what IndexManifest requires). */
    static String sha256(Path file) throws IOException {
        try (InputStream in = Files.newInputStream(file)) {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[8192];
            int n;
            while ((n = in.read(buffer)) > 0) {
                digest.update(buffer, 0, n);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}