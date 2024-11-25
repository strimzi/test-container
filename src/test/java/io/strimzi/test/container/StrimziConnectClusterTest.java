/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;


import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class StrimziConnectClusterTest {

    @Test
    void testConnectClusterNegativeOrZeroNumberOfWorkers() {
        assertThrows(IllegalArgumentException.class, () ->
                new StrimziConnectCluster.StrimziConnectClusterBuilder()
                        .withKafkaCluster(new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                                .withNumberOfBrokers(1)
                                .build())
                        .withNumberOfWorkers(0)
                        .withGroupId("groupId")
                        .build()
        );
        assertThrows(IllegalArgumentException.class, () ->
                new StrimziConnectCluster.StrimziConnectClusterBuilder()
                        .withKafkaCluster(new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                                .withNumberOfBrokers(1)
                                .build())
                        .withNumberOfWorkers(-1)
                        .withGroupId("groupId")
                        .build()
        );
    }

    @Test
    void testConnectClusterWithoutBootstrapServers() {
        assertThrows(IllegalArgumentException.class, () ->
                new StrimziConnectCluster.StrimziConnectClusterBuilder()
                        .withGroupId("groupId")
                        .withNumberOfWorkers(1)
                        .build()
        );
    }

    @Test
    void testConnectClusterWithoutGroupId() {
        assertThrows(IllegalArgumentException.class, () ->
                new StrimziConnectCluster.StrimziConnectClusterBuilder()
                        .withKafkaCluster(new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                                .withNumberOfBrokers(1)
                                .build())
                        .withNumberOfWorkers(1)
                        .build()
        );
    }

    @Test
    void testConnectCluster() {
        new StrimziConnectCluster.StrimziConnectClusterBuilder()
                .withKafkaCluster(new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                        .withNumberOfBrokers(1)
                        .build())
                .withGroupId("groupId")
                .build();
    }

    @Test
    void testGetRestApiEndpointThrowsBeforeStart() {
        StrimziConnectCluster cluster = new StrimziConnectCluster.StrimziConnectClusterBuilder()
                .withKafkaCluster(new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                        .withNumberOfBrokers(1)
                        .build())
                .withGroupId("groupId")
                .build();
        assertThrows(IllegalStateException.class, cluster::getRestEndpoint);
    }

    @Test
    void testAllBuilderMethods() {
        StrimziConnectCluster cluster = new StrimziConnectCluster.StrimziConnectClusterBuilder()
                .withKafkaCluster(new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                        .withNumberOfBrokers(1)
                        .build())
                .withNumberOfWorkers(2)
                .withGroupId("groupId")
                .withKafkaVersion("3.8.1")
                .withAdditionalConnectConfiguration(Map.of("plugin.path", "/tmp"))
                .withoutFileConnectors()
                .build();
    }

    @Test
    void testSetPluginPathWithFileConnector() {
        StrimziConnectCluster cluster = new StrimziConnectCluster.StrimziConnectClusterBuilder()
                .withKafkaCluster(new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                        .withNumberOfBrokers(1)
                        .build())
                .withNumberOfWorkers(2)
                .withGroupId("groupId")
                .withAdditionalConnectConfiguration(Map.of("plugin.path", "/tmp"))
                .build();
    }

    @Test
    void testWithAdditionalConnectConfigurationNull() {
        assertThrows(IllegalArgumentException.class, () ->
                new StrimziConnectCluster.StrimziConnectClusterBuilder()
                    .withKafkaCluster(new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                            .withNumberOfBrokers(1)
                            .build())
                    .withNumberOfWorkers(2)
                    .withGroupId("groupId")
                    .withAdditionalConnectConfiguration(null)
                    .build());
    }
}
