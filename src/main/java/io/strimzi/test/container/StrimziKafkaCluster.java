/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.lifecycle.Startables;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * A multi-node instance of Kafka using the latest image from quay.io/strimzi/kafka with the given version.
 * It perfectly fits for integration/system testing. The additional configuration for Kafka brokers can be passed to the constructor.
 * <br><br>
 */
public class StrimziKafkaCluster implements KafkaContainer {

    // class attributes
    private static final Logger LOGGER = LoggerFactory.getLogger(StrimziKafkaCluster.class);

    // instance attributes
    private final int brokersNum;
    private final int controllersNum;
    private final int internalTopicReplicationFactor;
    private final Map<String, String> additionalKafkaConfiguration;
    private final ToxiproxyContainer proxyContainer;
    private final boolean enableSharedNetwork;
    private final String kafkaVersion;
    private final boolean useSeparateRoles;

    // not editable
    private final Network network;
    private Collection<KafkaContainer> nodes;
    private Collection<KafkaContainer> controllers;
    private Collection<KafkaContainer> brokers;
    private final String clusterId;

    @SuppressWarnings("deprecation")
    private StrimziKafkaCluster(StrimziKafkaClusterBuilder builder) {
        this.brokersNum = builder.brokersNum;
        this.controllersNum = builder.controllersNum;
        this.useSeparateRoles = builder.useSeparateRoles;
        this.enableSharedNetwork = builder.enableSharedNetwork;
        this.network = this.enableSharedNetwork ? Network.SHARED : Network.newNetwork();
        
        // For separate roles, use controllersNum for replication factor calculation, otherwise use brokersNum
        int effectiveNodeCount = this.useSeparateRoles ? this.controllersNum : this.brokersNum;
        this.internalTopicReplicationFactor = builder.internalTopicReplicationFactor == 0 ? effectiveNodeCount : builder.internalTopicReplicationFactor;
        
        this.additionalKafkaConfiguration = builder.additionalKafkaConfiguration;
        this.proxyContainer = builder.proxyContainer;
        this.kafkaVersion = builder.kafkaVersion;
        this.clusterId = builder.clusterId;

        validateBrokerNum(this.brokersNum);
        if (this.useSeparateRoles) {
            validateControllerNum(this.controllersNum);
            validateInternalTopicReplicationFactor(this.internalTopicReplicationFactor, this.controllersNum);
        } else {
            validateInternalTopicReplicationFactor(this.internalTopicReplicationFactor, this.brokersNum);
        }

        if (this.proxyContainer != null) {
            this.proxyContainer.setNetwork(this.network);
        }

        prepareKafkaCluster(this.additionalKafkaConfiguration, this.kafkaVersion);
    }

    @SuppressWarnings("deprecation")
    private void prepareKafkaCluster(final Map<String, String> additionalKafkaConfiguration, final String kafkaVersion) {
        final Map<String, String> defaultKafkaConfigurationForMultiNode = new HashMap<>();
        defaultKafkaConfigurationForMultiNode.put("offsets.topic.replication.factor", String.valueOf(internalTopicReplicationFactor));
        defaultKafkaConfigurationForMultiNode.put("num.partitions", String.valueOf(internalTopicReplicationFactor));
        defaultKafkaConfigurationForMultiNode.put("transaction.state.log.replication.factor", String.valueOf(internalTopicReplicationFactor));
        defaultKafkaConfigurationForMultiNode.put("transaction.state.log.min.isr", String.valueOf(internalTopicReplicationFactor));

        // we have to configure quorum voters but also we simplify process because we use network aliases (i.e., broker-<id>)
        this.configureQuorumVoters(additionalKafkaConfiguration);

        if (additionalKafkaConfiguration != null) {
            defaultKafkaConfigurationForMultiNode.putAll(additionalKafkaConfiguration);
        }

        if (this.useSeparateRoles) {
            prepareSeparateRolesCluster(defaultKafkaConfigurationForMultiNode, kafkaVersion);
        } else {
            prepareMixedRolesCluster(defaultKafkaConfigurationForMultiNode, kafkaVersion);
        }
    }

    private void prepareMixedRolesCluster(final Map<String, String> kafkaConfiguration, final String kafkaVersion) {
        // multi-node set up with mixed roles (original behavior)
        this.nodes = IntStream
            .range(0, this.brokersNum)
            .mapToObj(brokerId -> {
                LOGGER.info("Starting mixed-role node with id {}", brokerId);
                // adding broker id for each kafka container
                StrimziKafkaContainer kafkaContainer = new StrimziKafkaContainer()
                    .withBrokerId(brokerId)
                    .withKafkaConfigurationMap(kafkaConfiguration)
                    .withNetwork(this.network)
                    .withProxyContainer(proxyContainer)
                    .withKafkaVersion(kafkaVersion == null ? KafkaVersionService.getInstance().latestRelease().getVersion() : kafkaVersion)
                    // One must set `node.id` to the same value as `broker.id` if we use KRaft mode
                    .withNodeId(brokerId)
                    // pass shared `cluster.id` to each broker
                    .withClusterId(this.clusterId)
                    .withNodeRole(KafkaNodeRole.MIXED)
                    .waitForRunning();

                LOGGER.info("Started mixed-role node with id: {}", kafkaContainer);

                return kafkaContainer;
            })
            .collect(Collectors.toList());
    }

    private void prepareSeparateRolesCluster(final Map<String, String> kafkaConfiguration, final String kafkaVersion) {
        // Create controller nodes - they get the first set of IDs
        this.controllers = IntStream
            .range(0, this.controllersNum)
            .mapToObj(controllerId -> {
                LOGGER.info("Starting controller-only node with id {}", controllerId);
                StrimziKafkaContainer controllerContainer = new StrimziKafkaContainer()
                    .withBrokerId(controllerId)
                    .withKafkaConfigurationMap(kafkaConfiguration)
                    .withNetwork(this.network)
                    .withKafkaVersion(kafkaVersion == null ? KafkaVersionService.getInstance().latestRelease().getVersion() : kafkaVersion)
                    .withNodeId(controllerId)
                    .withClusterId(this.clusterId)
                    .withNodeRole(KafkaNodeRole.CONTROLLER_ONLY)
                    .waitForRunning();

                LOGGER.info("Started controller-only node with id: {}", controllerContainer);
                return controllerContainer;
            })
            .collect(Collectors.toList());

        // Create broker nodes - use node IDs that don't conflict with controllers
        this.brokers = IntStream
            .range(0, this.brokersNum)
            .mapToObj(brokerIndex -> {
                // Use broker IDs that start after the highest controller ID to avoid conflicts
                int brokerId = this.controllersNum + brokerIndex;

                LOGGER.info("Starting broker-only node with broker.id={}", brokerId);
                
                StrimziKafkaContainer brokerContainer = new StrimziKafkaContainer()
                    .withBrokerId(brokerId)
                    .withKafkaConfigurationMap(kafkaConfiguration)
                    .withNetwork(this.network)
                    .withProxyContainer(proxyContainer)
                    .withKafkaVersion(kafkaVersion == null ? KafkaVersionService.getInstance().latestRelease().getVersion() : kafkaVersion)
                    .withNodeId(brokerId)
                    .withClusterId(this.clusterId)
                    .withNodeRole(KafkaNodeRole.BROKER_ONLY)
                    .waitForRunning();

                LOGGER.info("Started broker-only node with id: {}", brokerContainer);
                return brokerContainer;
            })
            .collect(Collectors.toList());

        // Combine all nodes for compatibility with existing methods
        this.nodes = new ArrayList<>();
        this.nodes.addAll(this.controllers);
        this.nodes.addAll(this.brokers);
    }

    private void validateBrokerNum(int brokersNum) {
        if (brokersNum <= 0) {
            throw new IllegalArgumentException("brokersNum '" + brokersNum + "' must be greater than 0");
        }
    }

    private void validateControllerNum(int controllersNum) {
        if (controllersNum <= 0) {
            throw new IllegalArgumentException("controllersNum '" + controllersNum + "' must be greater than 0");
        }
    }

    private void validateInternalTopicReplicationFactor(int internalTopicReplicationFactor, int brokersNum) {
        if (internalTopicReplicationFactor < 1 || internalTopicReplicationFactor > brokersNum) {
            throw new IllegalArgumentException(
                "internalTopicReplicationFactor '" + internalTopicReplicationFactor +
                    "' must be between 1 and " + brokersNum);
        }
    }

    /**
     * Builder class for {@code StrimziKafkaCluster}.
     * <p>
     * Use this builder to create instances of {@code StrimziKafkaCluster} with customized configurations.
     * </p>
     */
    public static class StrimziKafkaClusterBuilder {
        private int brokersNum;
        private int controllersNum = 0;
        private boolean useSeparateRoles;
        private int internalTopicReplicationFactor;
        private Map<String, String> additionalKafkaConfiguration = new HashMap<>();
        private ToxiproxyContainer proxyContainer;
        private boolean enableSharedNetwork;
        private String kafkaVersion;
        private String clusterId;

        /**
         * Sets the number of Kafka brokers in the cluster.
         *
         * @param brokersNum the number of Kafka brokers
         * @return the current instance of {@code StrimziConnectClusterBuilder} for method chaining
         */
        public StrimziKafkaClusterBuilder withNumberOfBrokers(int brokersNum) {
            this.brokersNum = brokersNum;
            return this;
        }

        /**
         * Sets the internal topic replication factor for Kafka brokers.
         * If not provided, it defaults to the number of brokers.
         *
         * @param internalTopicReplicationFactor the replication factor for internal topics
         * @return the current instance of {@code StrimziConnectClusterBuilder} for method chaining
         */
        public StrimziKafkaClusterBuilder withInternalTopicReplicationFactor(int internalTopicReplicationFactor) {
            this.internalTopicReplicationFactor = internalTopicReplicationFactor;
            return this;
        }

        /**
         * Adds additional Kafka configuration parameters.
         * These configurations are applied to all brokers in the cluster.
         *
         * @param additionalKafkaConfiguration a map of additional Kafka configuration options
         * @return the current instance of {@code StrimziConnectClusterBuilder} for method chaining
         */
        public StrimziKafkaClusterBuilder withAdditionalKafkaConfiguration(Map<String, String> additionalKafkaConfiguration) {
            if (additionalKafkaConfiguration != null) {
                this.additionalKafkaConfiguration.putAll(additionalKafkaConfiguration);
            }
            return this;
        }

        /**
         * Sets a {@code ToxiproxyContainer} to simulate network conditions such as latency or disconnection.
         *
         * @param proxyContainer the proxy container for simulating network conditions
         * @return the current instance of {@code StrimziConnectClusterBuilder} for method chaining
         */
        public StrimziKafkaClusterBuilder withProxyContainer(ToxiproxyContainer proxyContainer) {
            this.proxyContainer = proxyContainer;
            return this;
        }

        /**
         * Enables a shared Docker network for the Kafka cluster.
         * This allows the Kafka cluster to interact with other containers on the same network.
         *
         * @return the current instance of {@code StrimziConnectClusterBuilder} for method chaining
         */
        public StrimziKafkaClusterBuilder withSharedNetwork() {
            this.enableSharedNetwork = true;
            return this;
        }

        /**
         * Specifies the Kafka version to be used for the brokers in the cluster.
         * If no version is provided, the latest Kafka version available from {@link KafkaVersionService} will be used.
         *
         * @param kafkaVersion the desired Kafka version for the cluster
         * @return the current instance of {@code StrimziConnectClusterBuilder} for method chaining
         */
        public StrimziKafkaClusterBuilder withKafkaVersion(String kafkaVersion) {
            this.kafkaVersion = kafkaVersion;
            return this;
        }

        /**
         * Configures the cluster to use separate controller and broker nodes instead of mixed-role nodes.
         * When enabled, you must also specify the number of controllers using {@link #withNumberOfControllers(int)}.
         *
         * @return the current instance of {@code StrimziKafkaClusterBuilder} for method chaining
         */
        public StrimziKafkaClusterBuilder withSeparateRoles() {
            this.useSeparateRoles = true;
            return this;
        }

        /**
         * Sets the number of dedicated controller nodes when using separate roles.
         * This method should be used in conjunction with {@link #withSeparateRoles()}.
         *
         * @param controllersNum the number of dedicated controller nodes
         * @return the current instance of {@code StrimziKafkaClusterBuilder} for method chaining
         */
        public StrimziKafkaClusterBuilder withNumberOfControllers(int controllersNum) {
            if (controllersNum <= 0) {
                throw new IllegalArgumentException("controllersNum must be greater than 0");
            }
            this.controllersNum = controllersNum;
            return this;
        }

        /**
         * Builds and returns a {@code StrimziKafkaCluster} instance based on the provided configurations.
         *
         * @return a new instance of {@code StrimziKafkaCluster}
         * @throws IllegalStateException if separate roles is enabled but number of controllers is not specified
         */
        public StrimziKafkaCluster build() {
            // Validate separate roles configuration
            if (this.useSeparateRoles && this.controllersNum <= 0) {
                throw new IllegalStateException("When using separate roles, you must specify the number of controllers using withNumberOfControllers()");
            }
            
            // Generate a single cluster ID, which will be shared by all nodes
            this.clusterId = UUID.randomUUID().toString();

            return new StrimziKafkaCluster(this);
        }
    }

    /**
     * Returns the underlying GenericContainer instances for all Kafka nodes in the cluster.
     * In the current setup, all nodes are mixed-role (i.e., each acts as both broker and controller) in KRaft mode.
     *
     * @return Collection of GenericContainer representing the cluster nodes
     */
    public Collection<GenericContainer<?>> getNodes() {
        return nodes.stream()
            .map(node -> (GenericContainer<?>) node)
            .collect(Collectors.toList());
    }

    /**
     * Get the bootstrap servers that containers on the same network should use to connect
     * @return a comma separated list of Kafka bootstrap servers
     */
    @SuppressWarnings("deprecation")
    @DoNotMutate
    public String getNetworkBootstrapServers() {
        return getBrokers().stream()
                .map(broker -> ((StrimziKafkaContainer) broker).getNetworkBootstrapServers())
                .collect(Collectors.joining(","));
    }

    @Override
    public String getBootstrapServers() {
        return getBrokers().stream()
            .map(KafkaContainer::getBootstrapServers)
            .collect(Collectors.joining(","));
    }

    /* test */ int getInternalTopicReplicationFactor() {
        return this.internalTopicReplicationFactor;
    }

    /* test */ boolean isSharedNetworkEnabled() {
        return this.enableSharedNetwork;
    }

    /* test */ Map<String, String> getAdditionalKafkaConfiguration() {
        return this.additionalKafkaConfiguration;
    }

    @SuppressWarnings("deprecation")
    private void configureQuorumVoters(final Map<String, String> additionalKafkaConfiguration) {
        final String quorumVoters;
        
        if (this.useSeparateRoles) {
            // For separate roles, only controllers participate in the quorum
            quorumVoters = IntStream.range(0, this.controllersNum)
                .mapToObj(controllerId -> String.format("%d@" + StrimziKafkaContainer.NETWORK_ALIAS_PREFIX + "%d:9094", controllerId, controllerId))
                .collect(Collectors.joining(","));
        } else {
            // For mixed roles, all nodes participate in the quorum (original behavior)
            quorumVoters = IntStream.range(0, this.brokersNum)
                .mapToObj(brokerId -> String.format("%d@" + StrimziKafkaContainer.NETWORK_ALIAS_PREFIX + "%d:9094", brokerId, brokerId))
                .collect(Collectors.joining(","));
        }

        additionalKafkaConfiguration.put("controller.quorum.voters", quorumVoters);
    }

    @Override
    @DoNotMutate
    public void start() {
        // Start all Kafka containers
        Stream<KafkaContainer> startables = this.nodes.stream();
        try {
            Startables.deepStart(startables).get(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while starting Kafka containers", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to start Kafka containers", e);
        } catch (TimeoutException e) {
            throw new RuntimeException("Timed out while starting Kafka containers", e);
        }

        // Wait for quorum formation
        Utils.waitFor("Kafka brokers to form a quorum", Duration.ofSeconds(1), Duration.ofMinutes(1),
            this::checkAllBrokersReady);
    }

    @SuppressWarnings("deprecation")
    @DoNotMutate
    private boolean checkAllBrokersReady() {
        try {
            // Only check broker nodes for quorum readiness (if mixed-node then we check all nodes)
            Collection<KafkaContainer> brokersToCheck = getBrokers();
            
            for (KafkaContainer kafkaContainer : brokersToCheck) {
                if (!isBrokerReady((StrimziKafkaContainer) kafkaContainer)) {
                    return false;
                }
            }
            return true;
        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Failed to execute command in Kafka container", e);
        }
    }

    @SuppressWarnings("deprecation")
    @DoNotMutate
    private boolean isBrokerReady(StrimziKafkaContainer kafkaContainer) throws IOException, InterruptedException {
        Container.ExecResult result = kafkaContainer.execInContainer(
            "bash", "-c",
            "bin/kafka-metadata-quorum.sh --bootstrap-server localhost:" + StrimziKafkaContainer.INTER_BROKER_LISTENER_PORT + " describe --status"
        );
        String output = result.getStdout();

        LOGGER.info("Metadata quorum status from broker {}: {}", kafkaContainer.getBrokerId(), output);

        if (output == null || output.isEmpty()) {
            return false;
        }

        return isValidLeaderIdPresent(output);
    }

    @DoNotMutate
    private boolean isValidLeaderIdPresent(String output) {
        final Pattern leaderIdPattern = Pattern.compile("LeaderId:\\s+(\\d+)");
        final Matcher leaderIdMatcher = leaderIdPattern.matcher(output);

        if (!leaderIdMatcher.find()) {
            return false;
        }

        String leaderIdStr = leaderIdMatcher.group(1);
        try {
            int leaderId = Integer.parseInt(leaderIdStr);
            return leaderId >= 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    @Override
    @DoNotMutate
    public void stop() {
        // stop all kafka containers in parallel
        this.nodes.stream()
            .parallel()
            .forEach(KafkaContainer::stop);
    }

    protected Network getNetwork() {
        return network;
    }

    /**
     * Returns the controller nodes.
     * For mixed-role clusters, this returns all nodes.
     * For separate-role clusters, this returns only the controller-only nodes.
     *
     * @return Collection of controller nodes
     */
    public Collection<KafkaContainer> getControllerNodes() {
        if (this.useSeparateRoles) {
            return this.controllers;
        } else {
            return this.nodes;
        }
    }

    /**
     * Returns the broker nodes.
     * For mixed-role clusters, this returns all nodes.
     * For separate-role clusters, this returns only the broker-only nodes.
     *
     * Keep the method name getBrokers() to preserve backwards compatibility.
     *
     * @return Collection of broker nodes
     */
    public Collection<KafkaContainer> getBrokers() {
        if (this.useSeparateRoles) {
            return this.brokers;
        } else {
            return this.nodes;
        }
    }

    /**
     * Checks if the cluster is using separate controller and broker roles.
     *
     * @return true if using separate roles, false if using mixed roles
     */
    public boolean isUsingSeparateRoles() {
        return this.useSeparateRoles;
    }
}
