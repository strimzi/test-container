package io.strimzi.test.container;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class StrimziKafkaClusterTest {

    @Test
    void testKafkaClusterNegativeOrZeroNumberOfNodes() {
        assertThrows(IllegalArgumentException.class, () -> new StrimziKafkaCluster(
            0, 1, null, null));
        assertThrows(IllegalArgumentException.class, () -> new StrimziKafkaCluster(
            -1, 1, null, null));
    }

    @Test
    void testKafkaClusterPossibleNumberOfNodes() {
        assertDoesNotThrow(() -> new StrimziKafkaCluster(
            1, 1, null, null));
        assertDoesNotThrow(() -> new StrimziKafkaCluster(
            3, 3, null, null));
        assertDoesNotThrow(() -> new StrimziKafkaCluster(
            10, 3, null, null));
    }
}
