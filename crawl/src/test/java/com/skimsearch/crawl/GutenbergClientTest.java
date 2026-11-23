package com.skimsearch.crawl;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class GutenbergClientTest {

    @Test
    void splitsHeaderAndBody() {

        String text = """
                Title: Test Book
                Author: Test Author

                *** START OF THE PROJECT GUTENBERG EBOOK TEST ***

                Hello world.
                This is the body.

                *** END OF THE PROJECT GUTENBERG EBOOK TEST ***
                Footer
                """;

        GutenbergClient.BookContent result =
                GutenbergClient.split(text);

        assertTrue(result.header().contains("Title: Test Book"));
        assertTrue(result.header().contains("Author: Test Author"));

        assertTrue(result.body().contains("Hello world."));
        assertTrue(result.body().contains("This is the body."));

        assertFalse(result.body().contains("END OF"));
    }

    @Test
void splitsLegacyThisMarkerFormat() {

    String text = """
            Title: Legacy Test Book
            Author: Test Author

            *** START OF THIS PROJECT GUTENBERG EBOOK LEGACY TEST ***

            Legacy body content.

            *** END OF THIS PROJECT GUTENBERG EBOOK LEGACY TEST ***
            Footer
            """;

    GutenbergClient.BookContent result =
            GutenbergClient.split(text);

    assertTrue(result.header().contains("Title: Legacy Test Book"));
    assertTrue(result.header().contains("Author: Test Author"));
    assertTrue(result.body().contains("Legacy body content."));
    assertFalse(result.body().contains("END OF"));
}
}