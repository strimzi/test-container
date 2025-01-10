/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;


import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class StrimziConnectClusterTest {

    private static final String GROUP_ID = "groupId";
    private static final int WORKERS = 1;

    @Test
    void testConnectClusterNegativeOrZeroNumberOfWorkers() {
        assertThrows(IllegalArgumentException.class, () ->
                new StrimziConnectCluster.StrimziConnectClusterBuilder()
                        .withKafkaCluster(new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                                .withNumberOfBrokers(1)
                                .build())
                        .withNumberOfWorkers(0)
                        .withGroupId(GROUP_ID)
                        .build()
        );
        assertThrows(IllegalArgumentException.class, () ->
                new StrimziConnectCluster.StrimziConnectClusterBuilder()
                        .withKafkaCluster(new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                                .withNumberOfBrokers(1)
                                .build())
                        .withNumberOfWorkers(-1)
                        .withGroupId(GROUP_ID)
                        .build()
        );
    }

    @Test
    void testConnectClusterWithoutBootstrapServers() {
        assertThrows(IllegalArgumentException.class, () ->
                new StrimziConnectCluster.StrimziConnectClusterBuilder()
                        .withGroupId(GROUP_ID)
                        .withNumberOfWorkers(WORKERS)
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
                        .withNumberOfWorkers(WORKERS)
                        .build()
        );
    }

    @Test
    void testConnectCluster() {
        StrimziConnectCluster cluster = new StrimziConnectCluster.StrimziConnectClusterBuilder()
                .withKafkaCluster(new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                        .withNumberOfBrokers(1)
                        .build())
                .withGroupId(GROUP_ID)
                .build();
        assertThat(cluster.getWorkers().size(), is(WORKERS));
    }

    @Test
    void testGetRestApiEndpointThrowsBeforeStart() {
        StrimziConnectCluster cluster = new StrimziConnectCluster.StrimziConnectClusterBuilder()
                .withKafkaCluster(new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                        .withNumberOfBrokers(1)
                        .build())
                .withGroupId(GROUP_ID)
                .build();
        assertThat(cluster.getWorkers().size(), is(WORKERS));
        assertThrows(IllegalStateException.class, cluster::getRestEndpoint);
    }

    @Test
    void testAllBuilderMethods() {
        StrimziConnectCluster cluster = new StrimziConnectCluster.StrimziConnectClusterBuilder()
                .withKafkaCluster(new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                        .withNumberOfBrokers(1)
                        .build())
                .withNumberOfWorkers(2)
                .withGroupId(GROUP_ID)
                .withKafkaVersion("3.8.1")
                .withAdditionalConnectConfiguration(Map.of("plugin.path", "/tmp"))
                .withoutFileConnectors()
                .build();
        assertThat(cluster.getWorkers().size(), is(2));
    }

    @Test
    void testSetPluginPathWithFileConnector() {
        StrimziConnectCluster cluster = new StrimziConnectCluster.StrimziConnectClusterBuilder()
                .withKafkaCluster(new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                        .withNumberOfBrokers(1)
                        .build())
                .withNumberOfWorkers(2)
                .withGroupId(GROUP_ID)
                .withAdditionalConnectConfiguration(Map.of("plugin.path", "/tmp"))
                .build();
        assertThat(cluster.getWorkers().size(), is(2));
    }

    @Test
    void testWithAdditionalConnectConfigurationNull() {
        assertThrows(IllegalArgumentException.class, () ->
                new StrimziConnectCluster.StrimziConnectClusterBuilder()
                    .withKafkaCluster(new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                            .withNumberOfBrokers(1)
                            .build())
                    .withNumberOfWorkers(2)
                    .withGroupId(GROUP_ID)
                    .withAdditionalConnectConfiguration(null)
                    .build());
    }

    @Test
    void testWithEmptyAdditionalConnectConfiguration() {
        StrimziConnectCluster cluster = new StrimziConnectCluster.StrimziConnectClusterBuilder()
                .withKafkaCluster(new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                        .withNumberOfBrokers(1)
                        .build())
                .withGroupId(GROUP_ID)
                .withAdditionalConnectConfiguration(Map.of())
                .build();
        assertThat(cluster.getWorkers().size(), is(WORKERS));
    }
}
