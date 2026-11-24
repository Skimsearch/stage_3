package com.skimsearch.indexing;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** The canonical text of design B (contract, rule 4): the expected hash was computed independently in Python. */
class IndexDigestTest {

    @Test
    void sha256AndSizeOfTheCanonicalText() {
        IndexDigest digest = new IndexDigest();
        digest.addPostings("darcy", new int[] {1, 3, 7, 1});
        digest.addPostings("love", new int[] {2, 2});
        digest.addBook(1, "{\"bookId\":1}");
        digest.addBook(2, "{\"bookId\":2,\"title\":\"Alice\\u0027s\"}");

        // darcy<TAB>1,3,7,1<LF> love<TAB>2,2<LF> 1<TAB>{"bookId":1}<LF> 2<TAB>{...'...}<LF>
        assertEquals("9942d744bc6751fa46aa9eb53fe11f144cd7366423b7507ec93cb698b80897c7", digest.sha256());
        assertEquals(76, digest.sizeBytes());
    }
}
