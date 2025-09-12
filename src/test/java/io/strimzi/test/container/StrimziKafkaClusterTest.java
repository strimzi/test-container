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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
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
        assertThat(cluster.getNetwork().getId(), is(proxyContainer.getNetwork().getId()));
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

        assertThat(cluster.getNodes().size(), is(5));
        assertThat(cluster.getInternalTopicReplicationFactor(), is(3));
    }

    @Test
    void testKafkaClusterWithCustomNetworkConfiguration() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(4)
            .withSharedNetwork()
            .withInternalTopicReplicationFactor(2)
            .build();

        assertThat(cluster.getNodes().size(), is(4));
        assertThat(cluster.isSharedNetworkEnabled(), is(true));
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

        assertThat(cluster.getNodes().size(), is(3));
        assertThat(((StrimziKafkaContainer) cluster.getNodes().iterator().next()).getKafkaVersion(), is("3.7.1"));
        assertThat(cluster.getAdditionalKafkaConfiguration().get("log.retention.bytes"), is("10485760"));
        assertThat(
            ((StrimziKafkaContainer) cluster.getNodes().iterator().next())
                .getKafkaConfigurationMap()
                .get("log.retention.bytes"),
            is("10485760")
        );
    }

    @Test
    void testNetworkAssignmentBasedOnSharedNetworkFlag() {
        // Cluster with shared network enabled
        StrimziKafkaCluster sharedNetworkCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(1)
            .withSharedNetwork()
            .build();

        assertThat(sharedNetworkCluster.isSharedNetworkEnabled(), is(true));
        assertThat(sharedNetworkCluster.getNetwork(), is(Network.SHARED));

        // Cluster with shared network disabled
        StrimziKafkaCluster isolatedNetworkCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(1)
            .build();

        assertThat(isolatedNetworkCluster.isSharedNetworkEnabled(), is(false));
        assertThat(isolatedNetworkCluster.getNetwork(), is(CoreMatchers.not(Network.SHARED)));
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

        assertThat(cluster.getAdditionalKafkaConfiguration(), is(expectedConfig));
    }

    @Test
    void testQuorumVotersConfigurationIsNotEmpty() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(3)
            .build();

        String quorumVoters = cluster.getAdditionalKafkaConfiguration().get("controller.quorum.voters");
        assertThat(quorumVoters, is(CoreMatchers.notNullValue()));
        assertThat(quorumVoters, is(CoreMatchers.not(CoreMatchers.nullValue())));
        assertThat(quorumVoters, is("0@broker-0:9094,1@broker-1:9094,2@broker-2:9094"));
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
        assertThat(bootstrapServers, is(CoreMatchers.notNullValue()));
        assertThat(bootstrapServers, is(CoreMatchers.not(emptyString())));
        String[] servers = bootstrapServers.split(",");
        assertThat(servers.length, is(3));
    }

    @SuppressWarnings("deprecation")
    @Test
    void testGetNodesAndBrokersReturnsGenericContainers() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(2)
            .build();

        assertThat(cluster.getBrokers().size(), is(2));
        assertThat(cluster.getNodes().size(), is(2));

        for (GenericContainer<?> container : cluster.getNodes()) {
            assertThat(container, CoreMatchers.instanceOf(GenericContainer.class));
        }
    }

    @Test
    void testCombinedRolesClusterQuorumVoters() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(2)
            .withCombinedRoles()
            .withNumberOfControllers(3)
            .build();

        // Should only include controllers in quorum voters
        String expectedQuorumVoters = "0@broker-0:9094,1@broker-1:9094,2@broker-2:9094";
        assertThat(cluster.getAdditionalKafkaConfiguration().get("controller.quorum.voters"), is(expectedQuorumVoters));
    }

    @Test
    void testCombinedRolesClusterInternalTopicReplicationFactor() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(2)
            .withCombinedRoles()
            .withNumberOfControllers(3)
            .build();

        // Should use brokers count for replication factor calculation
        assertThat(cluster.getInternalTopicReplicationFactor(), is(2));
    }

    @Test
    void testCombinedRolesClusterWithCustomReplicationFactor() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(2)
            .withCombinedRoles()
            .withNumberOfControllers(3)
            .withInternalTopicReplicationFactor(2)
            .build();

        assertThat(cluster.getInternalTopicReplicationFactor(), is(2));
    }

    @Test
    void testCombinedRolesValidation() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(2)
                .withNumberOfControllers(0)
                .build()
        );
        assertThat(exception.getMessage(), CoreMatchers.containsString("controllersNum '0' must be greater than 0"));
    }

    @Test
    void testCombinedRolesValidationMissingControllers() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(2)
                .withCombinedRoles()
                .build()
        );
        assertThat(exception.getMessage(), CoreMatchers.containsString("controllersNum '0' must be greater than 0"));
    }

    @Test
    void testMixedRolesClusterDefault() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(3)
            .build();

        assertThat(cluster.isUsingCombinedRoles(), is(false));
        assertThat(cluster.getNodes().size(), is(3));
        assertThat(cluster.getBrokers().size(), is(3)); // All nodes are brokers in mixed mode
        assertThat(cluster.getControllerNodes().size(), is(3)); // All nodes are controllers in mixed mode
    }

    @Test
    void testCombinedRolesBootstrapServersOnlyFromBrokers() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(2)
            .withCombinedRoles()
            .withNumberOfControllers(1)
            .build();

        // Bootstrap servers should only come from broker nodes
        String bootstrapServers = cluster.getBootstrapServers();
        String networkBootstrapServers = cluster.getNetworkBootstrapServers();
        
        // Should have exactly 2 broker endpoints (not 3 total nodes)
        assertThat(bootstrapServers.split(",").length, is(2));
        assertThat(networkBootstrapServers.split(",").length, is(2));
    }

    @Test
    void testCombinedRolesClusterConfiguration() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(2)
            .withCombinedRoles()
            .withNumberOfControllers(3)
            .build();

        // Verify cluster configuration
        assertThat(cluster.isUsingCombinedRoles(), is(true));
        assertThat(cluster.getNodes().size(), is(5)); // 3 controllers + 2 brokers
        assertThat(cluster.getControllerNodes().size(), is(3));
        assertThat(cluster.getBrokers().size(), is(2));

        // Verify controller nodes have CONTROLLER role
        for (KafkaContainer controller : cluster.getControllerNodes()) {
            StrimziKafkaContainer container = (StrimziKafkaContainer) controller;
            assertThat(container.getNodeRole(), is(KafkaNodeRole.CONTROLLER));
        }

        // Verify broker nodes have BROKER role
        for (KafkaContainer broker : cluster.getBrokers()) {
            StrimziKafkaContainer container = (StrimziKafkaContainer) broker;
            assertThat(container.getNodeRole(), is(KafkaNodeRole.BROKER));
        }
    }

    @Test
    void testMixedRolesClusterConfiguration() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(3)
            .build();

        // Verify cluster configuration
        assertThat(cluster.isUsingCombinedRoles(), is(false));
        assertThat(cluster.getNodes().size(), is(3)); // 3 mixed-role nodes
        assertThat(cluster.getControllerNodes().size(), is(3)); // All nodes are controllers in mixed mode
        assertThat(cluster.getBrokers().size(), is(3)); // All nodes are brokers in mixed mode

        // Verify all nodes have MIXED role
        Collection<KafkaContainer> brokers = cluster.getBrokers();
        for (KafkaContainer node : brokers) {
            StrimziKafkaContainer container = (StrimziKafkaContainer) node;
            assertThat(container.getNodeRole(), is(KafkaNodeRole.MIXED));
        }
    }

    @Test
    void testCombinedRolesClusterWithNullKafkaVersion() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(2)
            .withCombinedRoles()
            .withNumberOfControllers(2)
            .withKafkaVersion(null) // Test null version edge case
            .build();
        
        // Should handle null version by using latest version
        assertThat(cluster.isUsingCombinedRoles(), is(true));
        assertThat(cluster.getControllerNodes().size(), is(2));
        assertThat(cluster.getBrokers().size(), is(2));
    }

    @Test
    void testCombinedRolesClusterBrokerIdCalculation() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(3)
            .withCombinedRoles()
            .withNumberOfControllers(5) // Test specific controller count
            .build();
        
        // Verify broker IDs start after controller count
        Collection<KafkaContainer> brokers = cluster.getBrokers();
        int expectedMinBrokerId = 5; // controllersNum
        for (KafkaContainer broker : brokers) {
            StrimziKafkaContainer container = (StrimziKafkaContainer) broker;
            assertThat(container.getBrokerId() >= expectedMinBrokerId, is(true));
        }
    }

    @Test
    void testCombinedRolesClusterWithVersionAndBrokerIdEdgeCases() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(1)
            .withCombinedRoles() 
            .withNumberOfControllers(1)
            .withKafkaVersion("3.8.0") // Test non-null version
            .build();
        
        // Test that version is properly set and broker ID calculation works with minimal setup
        assertThat(cluster.getBrokers().size(), is(1));
        assertThat(cluster.getControllerNodes().size(), is(1));
        
        StrimziKafkaContainer brokerContainer = (StrimziKafkaContainer) cluster.getBrokers().iterator().next();
        assertThat(brokerContainer.getBrokerId(), is(1)); // 1 controller + 0 index = 1
    }
}