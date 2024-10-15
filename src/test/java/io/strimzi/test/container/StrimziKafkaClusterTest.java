/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ToxiproxyContainer;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class StrimziKafkaClusterTest {

    @Test
    void testKafkaClusterNegativeOrZeroNumberOfNodes() {
        assertThrows(IllegalArgumentException.class, () ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(0)
                .withInternalTopicReplicationFactor(1)
                .build()
        );
        assertThrows(IllegalArgumentException.class, () ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(-1)
                .withInternalTopicReplicationFactor(1)
                .build()
        );
    }

    @Test
    void testKafkaClusterPossibleNumberOfNodes() {
        assertDoesNotThrow(() ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(1)
                .withInternalTopicReplicationFactor(1)
                .build()
        );
        assertDoesNotThrow(() ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(3)
                .withInternalTopicReplicationFactor(3)
                .build()
        );
        assertDoesNotThrow(() ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(10)
                .withInternalTopicReplicationFactor(3)
                .build()
        );
    }

    @Test
    void testNegativeOrMoreReplicasThanAvailableOfKafkaBrokersInternalReplicationError() {
        assertThrows(IllegalArgumentException.class, () ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(3)
                .withInternalTopicReplicationFactor(5)
                .build()
        );
        assertThrows(IllegalArgumentException.class, () ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(0)
                .withInternalTopicReplicationFactor(0)
                .build()
        );
    }

    @Test
    void testKafkaClusterWithProxyContainer() {
        ToxiproxyContainer proxyContainer = new ToxiproxyContainer();
        assertDoesNotThrow(() ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(3)
                .withInternalTopicReplicationFactor(3)
                .withProxyContainer(proxyContainer)
                .build()
        );
    }

    @Test
    void testKafkaClusterWithSharedNetwork() {
        assertDoesNotThrow(() ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(3)
                .withInternalTopicReplicationFactor(3)
                .withSharedNetwork()
                .build()
        );
    }

    @Test
    void testKafkaClusterWithAdditionalConfiguration() {
        Map<String, String> additionalConfig = new HashMap<>();
        additionalConfig.put("log.retention.ms", "60000");

        assertDoesNotThrow(() ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(3)
                .withInternalTopicReplicationFactor(3)
                .withAdditionalKafkaConfiguration(additionalConfig)
                .build()
        );
    }

    @Test
    void testKafkaClusterWithSpecificKafkaVersion() {
        assertDoesNotThrow(() ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(3)
                .withInternalTopicReplicationFactor(3)
                .withKafkaVersion("3.7.1")
                .build()
        );
    }

    @Test
    void testKafkaClusterWithMultipleBrokersAndReplicationFactor() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(5)
            .withInternalTopicReplicationFactor(3)
            .build();

        assertThat(cluster.getBrokers().size(), CoreMatchers.is(5));
        assertThat(cluster.getInternalTopicReplicationFactor(), CoreMatchers.is(3));
    }

    @Test
    void testKafkaClusterWithCustomNetworkConfiguration() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(4)
            .withSharedNetwork()
            .withInternalTopicReplicationFactor(2)
            .build();

        assertThat(cluster.getBrokers().size(), CoreMatchers.is(4));
        assertThat(cluster.isSharedNetworkEnabled(), CoreMatchers.is(true));
    }

    @Test
    void testKafkaClusterWithKafkaVersionAndAdditionalConfigs() {
        Map<String, String> additionalConfigs = new HashMap<>();
        additionalConfigs.put("log.retention.bytes", "10485760");

        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(3)
            .withInternalTopicReplicationFactor(3)
            .withKafkaVersion("3.7.1")
            .withAdditionalKafkaConfiguration(additionalConfigs)
            .build();

        assertThat(cluster.getBrokers().size(), CoreMatchers.is(3));
        assertThat(((StrimziKafkaContainer) cluster.getBrokers().iterator().next()).getKafkaVersion(), CoreMatchers.is("3.7.1"));
        assertThat(cluster.getAdditionalKafkaConfiguration().get("log.retention.bytes"), CoreMatchers.is("10485760"));
    }
}
