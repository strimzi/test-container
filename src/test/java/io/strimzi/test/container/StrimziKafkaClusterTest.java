/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyString;
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
    void testKafkaClusterWithProxyContainerAndKafkaClusterSetSameNetwork() {
        ToxiproxyContainer proxyContainer = new ToxiproxyContainer();

        assertThat(proxyContainer.getNetwork(), CoreMatchers.nullValue());

        StrimziKafkaCluster cluster =  new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(3)
            .withInternalTopicReplicationFactor(3)
            .withProxyContainer(proxyContainer)
            .withSharedNetwork()
            .build();

        System.out.println(proxyContainer.getNetwork());

        assertThat(cluster.getNetwork(), CoreMatchers.notNullValue());
        assertThat(proxyContainer.getNetwork(), CoreMatchers.notNullValue());
        assertThat(cluster.getNetwork().getId(), CoreMatchers.is(proxyContainer.getNetwork().getId()));
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

        assertThat(cluster.getNodes().size(), CoreMatchers.is(5));
        assertThat(cluster.getInternalTopicReplicationFactor(), CoreMatchers.is(3));
    }

    @Test
    void testKafkaClusterWithCustomNetworkConfiguration() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(4)
            .withSharedNetwork()
            .withInternalTopicReplicationFactor(2)
            .build();

        assertThat(cluster.getNodes().size(), CoreMatchers.is(4));
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

        assertThat(cluster.getNodes().size(), CoreMatchers.is(3));
        assertThat(((StrimziKafkaContainer) cluster.getNodes().iterator().next()).getKafkaVersion(), CoreMatchers.is("3.7.1"));
        assertThat(cluster.getAdditionalKafkaConfiguration().get("log.retention.bytes"), CoreMatchers.is("10485760"));
        assertThat(
            ((StrimziKafkaContainer) cluster.getNodes().iterator().next())
                .getKafkaConfigurationMap()
                .get("log.retention.bytes"),
            CoreMatchers.is("10485760")
        );
    }

    @Test
    void testNetworkAssignmentBasedOnSharedNetworkFlag() {
        // Cluster with shared network enabled
        StrimziKafkaCluster sharedNetworkCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(1)
            .withSharedNetwork()
            .build();

        assertThat(sharedNetworkCluster.isSharedNetworkEnabled(), CoreMatchers.is(true));
        assertThat(sharedNetworkCluster.getNetwork(), CoreMatchers.is(Network.SHARED));

        // Cluster with shared network disabled
        StrimziKafkaCluster isolatedNetworkCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(1)
            .build();

        assertThat(isolatedNetworkCluster.isSharedNetworkEnabled(), CoreMatchers.is(false));
        assertThat(isolatedNetworkCluster.getNetwork(), CoreMatchers.is(CoreMatchers.not(Network.SHARED)));
    }

    @Test
    void testBrokerNumValidation() {
        // Test with brokersNum = 0
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(0)
                .build()
        );
        assertThat(exception.getMessage(), CoreMatchers.containsString("brokersNum '0' must be greater than 0"));

        // Test with brokersNum = -1
        exception = assertThrows(IllegalArgumentException.class, () ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(-1)
                .build()
        );
        assertThat(exception.getMessage(), CoreMatchers.containsString("brokersNum '-1' must be greater than 0"));
    }

    @Test
    void testAdditionalKafkaConfigurationHandling() {
        // Null additional config
        StrimziKafkaCluster clusterWithNullConfig = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(3)
            .withInternalTopicReplicationFactor(3)
            .withAdditionalKafkaConfiguration(null)
            .build();

        assertThat(clusterWithNullConfig.getAdditionalKafkaConfiguration(), Matchers.hasEntry("controller.quorum.voters", "0@broker-0:9094,1@broker-1:9094,2@broker-2:9094"));

        // Non-null additional config
        Map<String, String> additionalConfig = new HashMap<>();
        additionalConfig.put("log.retention.ms", "60000");

        StrimziKafkaCluster clusterWithConfig = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(3)
            .withInternalTopicReplicationFactor(3)
            .withAdditionalKafkaConfiguration(additionalConfig)
            .build();

        assertThat(clusterWithConfig.getAdditionalKafkaConfiguration(), Matchers.hasEntry("log.retention.ms", "60000"));
    }

    @Test
    void testValidateBrokerNumBoundary() {
        // Test with brokersNum = 0 (should fail)
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(0)
                .build()
        );
        assertThat(exception.getMessage(), CoreMatchers.containsString("brokersNum '0' must be greater than 0"));

        // Test with brokersNum = 1 (should pass)
        assertDoesNotThrow(() ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(1)
                .build()
        );
    }

    @Test
    void testConfigureQuorumVotersIsCalledInKRaftMode() {
        Map<String, String> additionalConfig = new HashMap<>();
        additionalConfig.put("some.config", "someValue");

        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(3)
            .withAdditionalKafkaConfiguration(additionalConfig)
            .build();

        Map<String, String> expectedConfig = new HashMap<>(additionalConfig);
        expectedConfig.put("controller.quorum.voters", "0@broker-0:9094,1@broker-1:9094,2@broker-2:9094");

        assertThat(cluster.getAdditionalKafkaConfiguration(), CoreMatchers.is(expectedConfig));
    }

    @Test
    void testQuorumVotersConfigurationIsNotEmpty() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(3)
            .build();

        String quorumVoters = cluster.getAdditionalKafkaConfiguration().get("controller.quorum.voters");
        assertThat(quorumVoters, CoreMatchers.is(CoreMatchers.notNullValue()));
        assertThat(quorumVoters, CoreMatchers.is(CoreMatchers.not(CoreMatchers.nullValue())));
        assertThat(quorumVoters, CoreMatchers.is("0@broker-0:9094,1@broker-1:9094,2@broker-2:9094"));
    }

    @Test
    void testAdditionalKafkaConfigurationsAreApplied() {
        Map<String, String> additionalConfig = new HashMap<>();
        additionalConfig.put("log.retention.ms", "60000");
        additionalConfig.put("auto.create.topics.enable", "false");

        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(1)
            .withAdditionalKafkaConfiguration(additionalConfig)
            .build();

        assertThat(cluster.getAdditionalKafkaConfiguration(), CoreMatchers.allOf(
            Matchers.hasEntry("log.retention.ms", "60000"),
            Matchers.hasEntry("auto.create.topics.enable", "false")
        ));
    }

    @Test
    void testValidateInternalTopicReplicationFactorBoundaries() {
        // Test with internalTopicReplicationFactor = 0 (should fail)
        final int brokersNum = 3;
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(brokersNum)
                .withInternalTopicReplicationFactor(-1)
                .build()
        );
        assertThat(exception.getMessage(), CoreMatchers.containsString("internalTopicReplicationFactor '-1' must be between 1 and " + brokersNum));

        // Case: Replication factor is 0 (Defaults to brokersNum, should pass)
        assertDoesNotThrow(() ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(3)
                .withInternalTopicReplicationFactor(0)
                .build()
        );

        // Test with internalTopicReplicationFactor = 3 (equal to brokersNum, should pass)
        assertDoesNotThrow(() ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(3)
                .withInternalTopicReplicationFactor(3)
                .build()
        );

        // Test with internalTopicReplicationFactor = 4 (greater than brokersNum, should fail)
        exception = assertThrows(IllegalArgumentException.class, () ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(3)
                .withInternalTopicReplicationFactor(4)
                .build()
        );
        assertThat(exception.getMessage(), CoreMatchers.containsString("internalTopicReplicationFactor '4' must be between 1 and 3"));
    }

    @Test
    void testGetBootstrapServers() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(3)
            .build();

        String bootstrapServers = cluster.getBootstrapServers();
        assertThat(bootstrapServers, CoreMatchers.is(CoreMatchers.notNullValue()));
        assertThat(bootstrapServers, CoreMatchers.is(CoreMatchers.not(emptyString())));
        String[] servers = bootstrapServers.split(",");
        assertThat(servers.length, CoreMatchers.is(3));
    }

    @Test
    void testGetNodesAndBrokersReturnsGenericContainers() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(2)
            .build();

        assertThat(cluster.getBrokers().size(), CoreMatchers.is(2));
        assertThat(cluster.getNodes().size(), CoreMatchers.is(2));

        for (GenericContainer<?> container : cluster.getNodes()) {
            assertThat(container, CoreMatchers.instanceOf(GenericContainer.class));
        }
    }
}