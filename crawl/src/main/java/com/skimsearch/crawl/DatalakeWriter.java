package com.skimsearch.crawl;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.UUID;

public final class DatalakeWriter {

    public enum State {
        NOT_FOUND,
        INCOMPLETE,
        READY
    }

    private final Path root;

    public DatalakeWriter(Path root) {
        this.root = root;
    }

    public State state(int bookId) {

        boolean header = Files.isRegularFile(headerPath(bookId));
        boolean body = Files.isRegularFile(bodyPath(bookId));

        if (header && body) {
            return State.READY;
        }

        if (header || body) {
            return State.INCOMPLETE;
        }

        return State.NOT_FOUND;
    }

    public boolean write(
            int bookId,
            GutenbergClient.BookContent content
    ) {

        if (state(bookId) == State.READY) {
            return false;
        }

        Path folder = folder(bookId);

        String attempt = UUID.randomUUID().toString();

        Path headerTemp = folder.resolve(
                bookId + ".header." + attempt + ".tmp");

        Path bodyTemp = folder.resolve(
                bookId + ".body." + attempt + ".tmp");

        try {

            Files.createDirectories(folder);

            Files.writeString(
                    headerTemp,
                    content.header(),
                    StandardCharsets.UTF_8
            );

            Files.writeString(
                    bodyTemp,
                    content.body(),
                    StandardCharsets.UTF_8
            );

            moveAtomically(
                    headerTemp,
                    headerPath(bookId)
            );

            moveAtomically(
                    bodyTemp,
                    bodyPath(bookId)
            );

            return true;

        } catch (IOException e) {

            throw new UncheckedIOException(
                    "Cannot publish book " + bookId,
                    e
            );

        } finally {

            deleteQuietly(headerTemp);
            deleteQuietly(bodyTemp);
        }
    }

    Path headerPath(int bookId) {
        return folder(bookId)
                .resolve(bookId + ".header.txt");
    }

    Path bodyPath(int bookId) {
        return folder(bookId)
                .resolve(bookId + ".body.txt");
    }

    private Path folder(int bookId) {

        int start = (bookId / 1000) * 1000;
        int end = start + 999;

        return root
                .resolve("batch")
                .resolve(start + "-" + end);
    }

    private static void moveAtomically(
            Path source,
            Path destination
    ) throws IOException {

        try {

            Files.move(
                    source,
                    destination,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING
            );

        } catch (AtomicMoveNotSupportedException e) {

            Files.move(
                    source,
                    destination,
                    StandardCopyOption.REPLACE_EXISTING
            );
        }
    }

    private static void deleteQuietly(Path file) {

        try {
            Files.deleteIfExists(file);
        } catch (IOException ignored) {
            // Best-effort cleanup.
        }
    }
}