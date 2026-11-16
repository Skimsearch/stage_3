package com.skimsearch.search;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SearchConfigTest {

    @Test
    void consulRequiresAnExplicitAdvertiseHost() {
        var e = assertThrows(IllegalArgumentException.class,
                () -> SearchConfig.from(Map.of("CONSUL_URL", "http://127.0.0.1:8500")));
        assertEquals("CONSUL_URL is set but SEARCH_ADVERTISE_HOST is missing: set it to this machine's IP",
                e.getMessage());
    }

    @Test
    void consulWithAdvertiseHost() {
        var config = SearchConfig.from(Map.of(
                "CONSUL_URL", "http://127.0.0.1:8500",
                "SEARCH_ADVERTISE_HOST", "10.0.0.5"));
        assertEquals("10.0.0.5", config.advertiseHost());
    }

    @Test
    void withoutConsulNoAdvertiseHostIsNeeded() {
        assertFalse(SearchConfig.from(Map.of()).usesConsul());
    }
}