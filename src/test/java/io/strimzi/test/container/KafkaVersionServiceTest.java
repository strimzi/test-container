/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;

public class KafkaVersionServiceTest {

    private static final KafkaVersionService.KafkaVersion KAFKA_VERSION_3_3_1 = new KafkaVersionService.KafkaVersion("3.3.1", "custom-image1");
    private static final KafkaVersionService.KafkaVersion KAFKA_VERSION_3_2_0 = new KafkaVersionService.KafkaVersion("3.2.0", "custom-image2");

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

    @Test
    void testKafkaVersionAttributes() {
        assertThat(KAFKA_VERSION_3_3_1.getVersion(), CoreMatchers.is("3.3.1"));
        assertThat(KAFKA_VERSION_3_3_1.getImage(), CoreMatchers.is("custom-image1"));
        assertThat(KAFKA_VERSION_3_2_0.getVersion(), CoreMatchers.is("3.2.0"));
        assertThat(KAFKA_VERSION_3_2_0.getImage(), CoreMatchers.is("custom-image2"));
    }
}
