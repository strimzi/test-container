/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StrimziKafkaContainerTest {

    @Test
    public void testWriteOverrideString() {
        Map<String, String> map = new HashMap<>();
        map.put("foo", "bar");
        assertEquals(" --override 'foo=bar'", StrimziKafkaContainer.writeOverrideString(map));
        map.put("foo", "bar with spaces");
        assertEquals(" --override 'foo=bar with spaces'", StrimziKafkaContainer.writeOverrideString(map));
        map.put("foo", "bar with \"double quotes\"");
        assertEquals(" --override 'foo=bar with \"double quotes\"'", StrimziKafkaContainer.writeOverrideString(map));
        map.put("foo", "bar with 'single quotes'");
        assertEquals(" --override 'foo=bar with '\"'\"'single quotes'\"'\"''", StrimziKafkaContainer.writeOverrideString(map));
    }

}