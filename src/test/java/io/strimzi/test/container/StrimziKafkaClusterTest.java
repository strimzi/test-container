/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.toxiproxy.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyString;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class StrimziKafkaClusterTest {

    private static final DockerImageName TOXIPROXY_DOCKER_IMAGE_NAME = DockerImageName.parse("shopify/toxiproxy");

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
        ToxiproxyContainer proxyContainer = new ToxiproxyContainer(TOXIPROXY_DOCKER_IMAGE_NAME);
        assertDoesNotThrow(() ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(3)
                .withInternalTopicReplicationFactor(3)
                .withProxyContainer(proxyContainer)
                .build()
        );
    }

    @Test
    void testKafkaClusterWithProxyContainerConfiguresProxyPorts() {
        ToxiproxyContainer proxyContainer = new ToxiproxyContainer(TOXIPROXY_DOCKER_IMAGE_NAME);

        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(3)
            .withInternalTopicReplicationFactor(3)
            .withProxyContainer(proxyContainer)
            .build();

        // Verify that the proxy container has the correct exposed ports configured
        // For 3 brokers in combined mode, ports 8666, 8667, 8668 should be exposed
        // Note: ToxiproxyContainer pre-configures ports 8666-8695, so we can only verify presence
        List<Integer> exposedPorts = proxyContainer.getExposedPorts();
        assertThat(exposedPorts, CoreMatchers.notNullValue());
        assertThat(exposedPorts.contains(StrimziKafkaContainer.TOXIPROXY_PORT_BASE), is(true));
        assertThat(exposedPorts.contains(StrimziKafkaContainer.TOXIPROXY_PORT_BASE + 1), is(true));
        assertThat(exposedPorts.contains(StrimziKafkaContainer.TOXIPROXY_PORT_BASE + 2), is(true));
    }

    @Test
    void testKafkaClusterWithProxyContainerAndDedicatedRolesConfiguresProxyPorts() {
        ToxiproxyContainer proxyContainer = new ToxiproxyContainer(TOXIPROXY_DOCKER_IMAGE_NAME);

        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(2)
            .withDedicatedRoles()
            .withNumberOfControllers(3)
            .withProxyContainer(proxyContainer)
            .build();

        // Verify that the proxy container has the correct exposed ports configured
        // For 2 brokers + 3 controllers in dedicated mode, ports 8666-8670 should be exposed
        // Note: ToxiproxyContainer pre-configures ports 8666-8695, so we can only verify presence
        List<Integer> exposedPorts = cluster.getToxiproxyContainer().getExposedPorts();
        assertThat(exposedPorts, CoreMatchers.notNullValue());
        assertThat(exposedPorts.contains(StrimziKafkaContainer.TOXIPROXY_PORT_BASE), is(true));
        assertThat(exposedPorts.contains(StrimziKafkaContainer.TOXIPROXY_PORT_BASE + 1), is(true));
        assertThat(exposedPorts.contains(StrimziKafkaContainer.TOXIPROXY_PORT_BASE + 2), is(true));
        assertThat(exposedPorts.contains(StrimziKafkaContainer.TOXIPROXY_PORT_BASE + 3), is(true));
        assertThat(exposedPorts.contains(StrimziKafkaContainer.TOXIPROXY_PORT_BASE + 4), is(true));
    }

    @Test
    void testKafkaClusterWithProxyContainerSingleBrokerConfiguresProxyPorts() {
        ToxiproxyContainer proxyContainer = new ToxiproxyContainer(TOXIPROXY_DOCKER_IMAGE_NAME);

        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(1)
            .withProxyContainer(proxyContainer)
            .build();

        // Verify that the proxy container has the correct exposed ports configured
        // For 1 node in combined mode, port 8666 should be exposed
        // Note: ToxiproxyContainer pre-configures ports 8666-8695, so we can only verify presence
        List<Integer> exposedPorts = cluster.getToxiproxyContainer().getExposedPorts();
        assertThat(exposedPorts, CoreMatchers.notNullValue());
        assertThat(exposedPorts.contains(StrimziKafkaContainer.TOXIPROXY_PORT_BASE), is(true));
    }

    @Test
    void testKafkaClusterWithProxyContainerDedicatedRolesMinimumConfiguresProxyPorts() {
        ToxiproxyContainer proxyContainer = new ToxiproxyContainer(TOXIPROXY_DOCKER_IMAGE_NAME);

        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(1)
            .withDedicatedRoles()
            .withNumberOfControllers(1)
            .withProxyContainer(proxyContainer)
            .build();

        // Verify that the proxy container has the correct exposed ports configured
        // For 1 broker + 1 controller in dedicated mode, ports 8666-8667 should be exposed
        // Note: ToxiproxyContainer pre-configures ports 8666-8695, so we can only verify presence
        List<Integer> exposedPorts = cluster.getToxiproxyContainer().getExposedPorts();
        assertThat(exposedPorts, CoreMatchers.notNullValue());
        assertThat(exposedPorts.contains(StrimziKafkaContainer.TOXIPROXY_PORT_BASE), is(true));
        assertThat(exposedPorts.contains(StrimziKafkaContainer.TOXIPROXY_PORT_BASE + 1), is(true));
    }

    @Test
    void testKafkaClusterWithProxyContainerAndKafkaClusterSetSameNetwork() {
        ToxiproxyContainer proxyContainer = new ToxiproxyContainer(TOXIPROXY_DOCKER_IMAGE_NAME);

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
        assertThat(cluster.getToxiproxyContainer(), CoreMatchers.notNullValue());
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
        assertThat(cluster.getNodes().iterator().next().getKafkaVersion(), is("3.7.1"));
        assertThat(cluster.getAdditionalKafkaConfiguration().get("log.retention.bytes"), is("10485760"));
        assertThat(cluster.getNodes().iterator().next().getKafkaConfigurationMap().get("log.retention.bytes"),
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

    @Test
    void testCombinedRolesClusterQuorumVoters() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(2)
            .withDedicatedRoles()
            .withNumberOfControllers(3)
            .build();

        // Should only include controllers in quorum voters
        String expectedQuorumVoters = "0@broker-0:9094,1@broker-1:9094,2@broker-2:9094";
        assertThat(cluster.getAdditionalKafkaConfiguration().get("controller.quorum.voters"), is(expectedQuorumVoters));
    }

    @Test
    void testDedicatedRolesClusterInternalTopicReplicationFactor() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(2)
            .withDedicatedRoles()
            .withNumberOfControllers(3)
            .build();

        // Should use brokers count for replication factor calculation
        assertThat(cluster.getInternalTopicReplicationFactor(), is(2));
    }

    @Test
    void testDedicatedRolesClusterWithCustomReplicationFactor() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(2)
            .withDedicatedRoles()
            .withNumberOfControllers(3)
            .withInternalTopicReplicationFactor(2)
            .build();

        assertThat(cluster.getInternalTopicReplicationFactor(), is(2));
    }

    @Test
    void testDedicatedRolesValidation() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(2)
                .withNumberOfControllers(0)
                .withDedicatedRoles()
                .build()
        );
        assertThat(exception.getMessage(), CoreMatchers.containsString("controllersNum '0' must be greater than 0"));
    }

    @Test
    void testDedicatedRolesValidationMissingControllers() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(2)
                .withDedicatedRoles()
                .build()
        );
        assertThat(exception.getMessage(), CoreMatchers.containsString("controllersNum '0' must be greater than 0"));
    }

    @Test
    void testCombinedRolesClusterDefault() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(3)
            .build();

        assertThat(cluster.isUsingDedicatedRoles(), is(false));
        assertThat(cluster.getNodes().size(), is(3));
        assertThat(cluster.getBrokers().size(), is(3)); // All nodes are brokers in combined mode
        assertThat(cluster.getControllers().size(), is(3)); // All nodes are controllers in combined mode
    }

    @Test
    void testDedicatedRolesBootstrapServersOnlyFromBrokers() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(2)
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
    void testDedicatedRolesClusterConfiguration() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(2)
            .withDedicatedRoles()
            .withNumberOfControllers(3)
            .build();

        // Verify cluster configuration
        assertThat(cluster.isUsingDedicatedRoles(), is(true));
        assertThat(cluster.getNodes().size(), is(5)); // 3 controllers + 2 brokers
        assertThat(cluster.getControllers().size(), is(3));
        assertThat(cluster.getBrokers().size(), is(2));

        // Verify controller nodes have CONTROLLER role
        for (StrimziKafkaContainer container : cluster.getControllers()) {
            assertThat(container.getNodeRole(), is(KafkaNodeRole.CONTROLLER));
        }

        // Verify broker nodes have BROKER role
        for (StrimziKafkaContainer container : cluster.getBrokers()) {
            assertThat(container.getNodeRole(), is(KafkaNodeRole.BROKER));
        }
    }

    @Test
    void testCombinedRolesClusterConfiguration() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(3)
            .build();

        // Verify cluster configuration
        assertThat(cluster.isUsingDedicatedRoles(), is(false));
        assertThat(cluster.getNodes().size(), is(3)); // 3 combined-role nodes
        assertThat(cluster.getControllers().size(), is(3)); // All nodes are controllers in combined mode
        assertThat(cluster.getBrokers().size(), is(3)); // All nodes are brokers in combined mode

        // Verify all nodes have combined role
        for (StrimziKafkaContainer container : cluster.getBrokers()) {
            assertThat(container.getNodeRole(), is(KafkaNodeRole.COMBINED));
        }
    }

    @Test
    void testDedicatedRolesClusterWithNullKafkaVersion() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(2)
            .withDedicatedRoles()
            .withNumberOfControllers(2)
            .withKafkaVersion(null) // Test null version edge case
            .build();
        
        // Should handle null version by using latest version
        assertThat(cluster.isUsingDedicatedRoles(), is(true));
        assertThat(cluster.getControllers().size(), is(2));
        assertThat(cluster.getBrokers().size(), is(2));
    }

    @Test
    void testDedicatedRolesClusterNodeIdCalculation() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(3)
            .withDedicatedRoles()
            .withNumberOfControllers(5) // Test specific controller count
            .build();
        
        // Verify node IDs start after controller count
        int expectedMinNodeId = 5; // controllersNum
        for (StrimziKafkaContainer container : cluster.getBrokers()) {
            assertThat(container.getNodeId() >= expectedMinNodeId, is(true));
        }
    }

    @Test
    void testDedicatedRolesClusterWithVersionAndNodeIdEdgeCases() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(1)
            .withDedicatedRoles()
            .withNumberOfControllers(1)
            .withKafkaVersion("3.8.0") // Test non-null version
            .build();
        
        // Test that version is properly set and node ID calculation works with minimal setup
        assertThat(cluster.getBrokers().size(), is(1));
        assertThat(cluster.getControllers().size(), is(1));
        
        StrimziKafkaContainer brokerContainer = cluster.getBrokers().iterator().next();
        assertThat(brokerContainer.getNodeId(), is(1)); // 1 controller + 0 index = 1
    }

    @Test
    void testGetBootstrapControllersWithCombinedRoles() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(3)
            .build();

        String bootstrapControllers = cluster.getBootstrapControllers();
        assertThat(bootstrapControllers, is(CoreMatchers.notNullValue()));
        assertThat(bootstrapControllers, is(CoreMatchers.not(emptyString())));
        
        String[] controllers = bootstrapControllers.split(",");
        assertThat(controllers.length, is(3));
    }

    @Test
    void testGetBootstrapControllersWithDedicatedRoles() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(2)
            .withDedicatedRoles()
            .withNumberOfControllers(3)
            .build();

        String bootstrapControllers = cluster.getBootstrapControllers();
        assertThat(bootstrapControllers, is(CoreMatchers.notNullValue()));
        assertThat(bootstrapControllers, is(CoreMatchers.not(emptyString())));
        
        String[] controllers = bootstrapControllers.split(",");
        assertThat(controllers.length, is(3));
    }

    @Test
    void testGetBootstrapControllersWithSingleController() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(1)
            .withDedicatedRoles()
            .withNumberOfControllers(1)
            .build();

        String bootstrapControllers = cluster.getBootstrapControllers();
        assertThat(bootstrapControllers, is(CoreMatchers.notNullValue()));
        assertThat(bootstrapControllers, is(CoreMatchers.not(emptyString())));

        String[] controllers = bootstrapControllers.split(",");
        assertThat(controllers.length, is(1));
    }

    @Test
    void testWithLogCollection() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(3)
            .withLogCollection()
            .build();

        assertThat(cluster, is(CoreMatchers.notNullValue()));
    }

    @Test
    void testWithLogFilePathValid() {
        assertDoesNotThrow(() ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(3)
                .withLogCollection("target/test-logs/")
                .build()
        );
    }

    @Test
    void testWithLogFilePathNullThrowsException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(3)
                .withLogCollection(null)
                .build()
        );
        assertThat(exception.getMessage(), CoreMatchers.containsString("Log file path cannot be null or empty"));
    }

    @Test
    void testWithLogFilePathEmptyThrowsException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(3)
                .withLogCollection("")
                .build()
        );
        assertThat(exception.getMessage(), CoreMatchers.containsString("Log file path cannot be null or empty"));
    }

    @Test
    void testWithLogFilePathWhitespaceOnlyThrowsException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(3)
                .withLogCollection("   ")
                .build()
        );
        assertThat(exception.getMessage(), CoreMatchers.containsString("Log file path cannot be null or empty"));
    }

    @Test
    void testDedicatedRolesClusterWithLogFilePath() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(2)
            .withDedicatedRoles()
            .withNumberOfControllers(2)
            .withLogCollection("target/dedicated-roles-logs/")
            .build();

        assertThat(cluster.isUsingDedicatedRoles(), is(true));
        assertThat(cluster.getControllers().size(), is(2));
        assertThat(cluster.getBrokers().size(), is(2));
        assertThat(cluster.getNodes().size(), is(4));
    }

    @Test
    void testWithPortValidation() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(3)
                .withPort(0)
                .build()
        );
        assertThat(exception.getMessage(), CoreMatchers.containsString("The fixed Kafka port must be greater than 0"));

        exception = assertThrows(IllegalArgumentException.class, () ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(3)
                .withPort(-1)
                .build()
        );
        assertThat(exception.getMessage(), CoreMatchers.containsString("The fixed Kafka port must be greater than 0"));
    }

    @Test
    void testCombinedRolesClusterWithPortIncrement() {
        int basePort = 19092;
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(3)
            .withPort(basePort)
            .build();

        int nodeIndex = 0;
        for (StrimziKafkaContainer container : cluster.getNodes()) {
            int nodeId = container.getNodeId();
            int expectedPort = basePort + nodeId;
            assertThat("Node " + nodeId + " should have fixed exposed port " + expectedPort,
                container.getFixedExposedPort(), is(expectedPort));
            nodeIndex++;
        }
        assertThat(nodeIndex, is(3));
    }

    @Test
    void testDedicatedRolesClusterWithPortIncrementOnlyBrokers() {
        int basePort = 19092;
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(2)
            .withDedicatedRoles()
            .withNumberOfControllers(3)
            .withPort(basePort)
            .build();

        int brokerIndex = 0;
        for (StrimziKafkaContainer container : cluster.getBrokers()) {
            int expectedPort = basePort + brokerIndex;
            assertThat("Broker " + brokerIndex + " should have fixed exposed port " + expectedPort,
                container.getFixedExposedPort(), is(expectedPort));
            brokerIndex++;
        }
        assertThat(brokerIndex, is(2));

        // Verify controllers do NOT have fixed ports (port increment only applies to brokers)
        for (StrimziKafkaContainer container : cluster.getControllers()) {
            assertThat("Controller should not have a fixed exposed port",
                container.getFixedExposedPort(), is(0));
        }
    }

    @Test
    void testDedicatedRolesClusterWithBootstrapServersProviderAppliedToBrokers() {
        Function<StrimziKafkaContainer, String> customProvider =
            c -> String.format("PLAINTEXT://custom-host-%d:%d", c.getNodeId(), StrimziKafkaContainer.KAFKA_PORT);

        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(2)
            .withDedicatedRoles()
            .withNumberOfControllers(2)
            .withBootstrapServers(customProvider)
            .build();

        // Verify bootstrapServersProvider is applied to brokers
        for (StrimziKafkaContainer container : cluster.getBrokers()) {
            assertThat("Broker should have custom bootstrapServersProvider set",
                container.getBootstrapServersProvider(), is(customProvider));
        }
    }

    @Test
    void testCombinedRolesClusterWithBootstrapServersProviderApplied() {
        Function<StrimziKafkaContainer, String> customProvider =
            c -> String.format("PLAINTEXT://custom-host-%d:%d", c.getNodeId(), StrimziKafkaContainer.KAFKA_PORT);

        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(3)
            .withBootstrapServers(customProvider)
            .build();

        // Verify bootstrapServersProvider is applied to all nodes in combined mode
        for (StrimziKafkaContainer container : cluster.getNodes()) {
            assertThat("Node should have custom bootstrapServersProvider set",
                container.getBootstrapServersProvider(), is(customProvider));
        }
    }

    @Test
    void testDedicatedRolesClusterWithContainerCustomizerAppliedToBrokers() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(2)
                .withDedicatedRoles()
                .withNumberOfControllers(2)
                .withContainerCustomizer(c -> c.withLabel("custom-label", "custom-value"))
                .build();

        // Verify bootstrapServersProvider is applied to brokers
        for (StrimziKafkaContainer container : cluster.getBrokers()) {
            assertThat("Broker should have custom bootstrapServersProvider set",
                    container.getLabels().get("custom-label"), is("custom-value"));
        }
    }


    @Test
    void testCombinedRolesClusterWithContainerCustomizerApplied() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(3)
            .withContainerCustomizer(c -> c.withLabel("custom-label", "custom-value"))
            .build();

        // Verify bootstrapServersProvider is applied to all nodes in combined mode
        for (StrimziKafkaContainer container : cluster.getNodes()) {
            assertThat("Node should have custom bootstrapServersProvider set",
                container.getLabels().get("custom-label"), is("custom-value"));
        }
    }

    @Test
    void testDedicatedRolesClusterWithoutLogFilePathBrokersHaveNullPath() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(2)
            .withDedicatedRoles()
            .withNumberOfControllers(2)
            .build();

        for (StrimziKafkaContainer container : cluster.getBrokers()) {
            assertThat("Broker should not have logFilePath set when not configured",
                container.getLogFilePath(), is(CoreMatchers.nullValue()));
        }
    }

    @Test
    void testClusterWithSaslUsernameWhitespaceOnlyThrowsException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(2)
                .withSaslUsername("   ")
                .build()
        );
        assertThat(exception.getMessage(), CoreMatchers.containsString("SASL username cannot be null or empty"));
    }

    @Test
    void testClusterWithSaslPasswordWhitespaceOnlyThrowsException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(2)
                .withSaslPassword("   ")
                .build()
        );
        assertThat(exception.getMessage(), CoreMatchers.containsString("SASL password cannot be null or empty"));
    }

    @Test
    void testOAuthConfigIsAppliedToDedicatedRolesBrokers() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(2)
            .withDedicatedRoles()
            .withNumberOfControllers(2)
            .withOAuthConfig("test-realm", "test-client-id", "test-client-secret",
                "http://oauth-server:8080", "preferred_username")
            .build();

        // Verify OAuth config is propagated to broker nodes
        for (StrimziKafkaContainer container : cluster.getBrokers()) {
            assertThat("OAuth should be enabled on broker", container.isOAuthEnabled(), is(true));
        }
    }

    @Test
    void testSaslCredentialsAreAppliedToDedicatedRolesBrokers() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(2)
            .withDedicatedRoles()
            .withNumberOfControllers(2)
            .withSaslUsername("test-user")
            .withSaslPassword("test-password")
            .build();

        // Verify SASL credentials are propagated to broker nodes
        for (StrimziKafkaContainer container : cluster.getBrokers()) {
            assertThat("SASL username should be set on broker",
                container.getSaslUsername(), is("test-user"));
            assertThat("SASL password should be set on broker",
                container.getSaslPassword(), is("test-password"));
        }
    }
    @Test
    void testWithAuthenticationTypeNullDoesNotOverridePreviousValue() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(1)
            .withAuthenticationType(AuthenticationType.OAUTH_BEARER)
            .withAuthenticationType(null) // This should NOT override the previous value
            .build();

        for (StrimziKafkaContainer container : cluster.getNodes()) {
            assertThat("Authentication type should still be OAUTH_BEARER after null call",
                container.getAuthenticationType(), is(AuthenticationType.OAUTH_BEARER));
        }
    }

    @Test
    void testWithImageValidImageName() {
        String customImage = "quay.io/strimzi/kafka:0.49.0-kafka-4.1.0";
        assertDoesNotThrow(() ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(2)
                .withImage(customImage)
                .build()
        );
    }

    @Test
    void testWithImageNullThrowsException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(2)
                .withImage(null)
                .build()
        );
        assertThat(exception.getMessage(), CoreMatchers.containsString("Kafka image cannot be null or empty"));
    }

    @Test
    void testWithImageDedicatedRolesCluster() {
        String customImage = "quay.io/strimzi/kafka:0.49.0-kafka-4.1.0";
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(2)
            .withDedicatedRoles()
            .withNumberOfControllers(2)
            .withImage(customImage)
            .build();

        assertThat(cluster.isUsingDedicatedRoles(), is(true));
        assertThat(cluster.getNodes().size(), is(4));
    }

    @Test
    void testWithoutImageDedicatedRolesSetsKafkaVersion() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(2)
            .withDedicatedRoles()
            .withNumberOfControllers(2)
            .withKafkaVersion("4.1.0")
            .build();

        // Verify kafkaVersion => set on controller containers
        for (StrimziKafkaContainer container : cluster.getControllers()) {
            assertThat("Controller should have kafkaVersion set",
                container.getKafkaVersion(), is("4.1.0"));
        }

        // Verify kafkaVersion => set on broker containers
        for (StrimziKafkaContainer container : cluster.getBrokers()) {
            assertThat("Broker should have kafkaVersion set",
                container.getKafkaVersion(), is("4.1.0"));
        }
    }

    @Test
    void testDedicatedRolesClusterWithLogFilePathAppliesToControllers() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(2)
            .withDedicatedRoles()
            .withNumberOfControllers(2)
            .withLogCollection("target/controller-logs/")
            .build();

        // Verify logFilePath is set on controller containers
        for (StrimziKafkaContainer container : cluster.getControllers()) {
            assertThat("Controller should have logFilePath set",
                container.getLogFilePath(), is("target/controller-logs/"));
        }

        // Verify logFilePath is set on broker containers
        for (StrimziKafkaContainer container : cluster.getBrokers()) {
            assertThat("Broker should have logFilePath set",
                container.getLogFilePath(), is("target/controller-logs/"));
        }
    }

    @Test
    void testGetSupportedKafkaVersionsReturnsNonEmptyList() {
        List<String> versions = StrimziKafkaCluster.getSupportedKafkaVersions();

        assertThat(versions, is(CoreMatchers.notNullValue()));
        assertThat(versions.isEmpty(), is(false));
    }

    @Test
    void testGetSupportedKafkaVersionsAreSorted() {
        List<String> versions = StrimziKafkaCluster.getSupportedKafkaVersions();

        for (int i = 0; i < versions.size() - 1; i++) {
            String current = versions.get(i);
            String next = versions.get(i + 1);
            int comparison = KafkaVersionService.KafkaVersion.compareVersions(current, next);
            assertThat("Versions should be sorted: " + current + " should be <= " + next,
                comparison <= 0, is(true));
        }
    }

    @Test
    void testGetSupportedKafkaVersionsContainsValidVersionFormat() {
        List<String> versions = StrimziKafkaCluster.getSupportedKafkaVersions();

        for (String version : versions) {
            assertThat("Version should match X.Y.Z format: " + version,
                version.matches("\\d+\\.\\d+\\.\\d+"), is(true));
        }
    }
}