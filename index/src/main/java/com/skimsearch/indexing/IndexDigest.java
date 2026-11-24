package com.skimsearch.indexing;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * SHA-256 and size of the canonical text of a design B version (contract, rule 4):
 * one line per term of postings-v{N} sorted by term, "term TAB v1,v2,...,vn LF", followed by
 * one line per book sorted by bookId, "bookId TAB json LF", with the JSON byte for byte as stored.
 * Lines must be added in that order. Text is UTF-8.
 */
final class IndexDigest {

    private final MessageDigest sha;
    private long size;

    IndexDigest() {
        try {
            sha = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    void addPostings(String term, int[] values) {
        StringBuilder line = new StringBuilder(term).append('\t');
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                line.append(',');
            }
            line.append(values[i]);
        }
        add(line.append('\n').toString());
    }

    void addBook(int bookId, String json) {
        add(bookId + "\t" + json + "\n");
    }

    private void add(String line) {
        byte[] bytes = line.getBytes(StandardCharsets.UTF_8);
        sha.update(bytes);
        size += bytes.length;
    }

    /** Call once, after the last line. */
    String sha256() {
        return HexFormat.of().formatHex(sha.digest());
    }

    long sizeBytes() {
        return size;
    }
}
