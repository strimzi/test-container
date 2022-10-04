/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;

public class KafkaVersionServiceTest {

    private static final KafkaVersionService.KafkaVersion KAFKA_VERSION_3_3_1 = new KafkaVersionService.KafkaVersion("3.3.1", "custom-image");
    private static final KafkaVersionService.KafkaVersion KAFKA_VERSION_3_2_0 = new KafkaVersionService.KafkaVersion("3.2.0", "custom-image");

    @Test
    void testKafkaVersionsComparisonLess() {
        assertThat(KAFKA_VERSION_3_2_0.compareTo(KAFKA_VERSION_3_3_1), CoreMatchers.is(-1));
    }

    @Test
    void testKafkaVersionsComparisonGreater() {
        assertThat(KAFKA_VERSION_3_3_1.compareTo(KAFKA_VERSION_3_2_0), CoreMatchers.is(1));
    }

    @Test
    void testKafkaVersionsComparisonEqual() {
        assertThat(KAFKA_VERSION_3_3_1.compareTo(KAFKA_VERSION_3_3_1), CoreMatchers.is(0));
        assertThat(KAFKA_VERSION_3_2_0.compareTo(KAFKA_VERSION_3_2_0), CoreMatchers.is(0));
    }
}
