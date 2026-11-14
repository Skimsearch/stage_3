package com.skimsearch.common;

import java.util.Objects;

/**
 * Optional search filters. A null field means "do not filter by it".
 * author and language: exact match, case-insensitive only for A-Z (see sameText).
 * year: exact match.
 */
public record Filters(String author, String language, Integer year) {

    private static final Filters NONE = new Filters(null, null, null);

    public static Filters none() {
        return NONE;
    }

    /** True if a book with these metadata passes the filters (for in-memory indexes, design B). */
    public boolean matches(String bookAuthor, String bookLanguage, Integer bookYear) {
        return (author == null || sameText(author, bookAuthor))
                && (language == null || sameText(language, bookLanguage))
                && (year == null || Objects.equals(year, bookYear));
    }

    /**
     * Same rule as SQLite's LOWER(a) = LOWER(b): only A-Z are folded to lower case,
     * every other character (É, é, Ñ...) must match exactly. A null never matches,
     * like NULL = x in SQL. Do not replace with equalsIgnoreCase or toLowerCase:
     * they fold non-ASCII letters too and A and B would return different results.
     */
    public static boolean sameText(String a, String b) {
        if (a == null || b == null || a.length() != b.length()) {
            return false;
        }
        for (int i = 0; i < a.length(); i++) {
            if (asciiLower(a.charAt(i)) != asciiLower(b.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static char asciiLower(char c) {
        return (c >= 'A' && c <= 'Z') ? (char) (c + ('a' - 'A')) : c;
    }
}