/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import eu.rekawek.toxiproxy.Proxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.toxiproxy.ToxiproxyContainer;
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
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * A multi-node instance of Kafka using the latest image from quay.io/strimzi/kafka with the given version.
 * It perfectly fits for integration/system testing.
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
    private final boolean useDedicatedRoles;
    private final String logFilePath;
    private final int fixedExposedPort;
    private final Function<StrimziKafkaContainer, String> bootstrapServersProvider;

    // not editable
    private final Network network;
    private Collection<KafkaContainer> nodes;
    private Collection<KafkaContainer> controllers;
    private Collection<KafkaContainer> brokers;
    private final String clusterId;

    private StrimziKafkaCluster(StrimziKafkaClusterBuilder builder) {
        this.brokersNum = builder.brokersNum;
        this.controllersNum = builder.controllersNum;
        this.useDedicatedRoles = builder.useDedicatedRoles;
        this.enableSharedNetwork = builder.enableSharedNetwork;
        this.network = this.enableSharedNetwork ? Network.SHARED : Network.newNetwork();

        // replication factor must be <= number of brokers
        this.internalTopicReplicationFactor = builder.internalTopicReplicationFactor == 0 ? this.brokersNum : builder.internalTopicReplicationFactor;

        this.additionalKafkaConfiguration = builder.additionalKafkaConfiguration;
        this.proxyContainer = builder.proxyContainer;
        this.kafkaVersion = builder.kafkaVersion;
        this.clusterId = builder.clusterId;
        this.logFilePath = builder.logFilePath;
        this.fixedExposedPort = builder.fixedExposedPort;
        this.bootstrapServersProvider = builder.bootstrapServersProvider;

        validateBrokerNum(this.brokersNum);
        if (this.isUsingDedicatedRoles()) {
            validateControllerNum(this.controllersNum);
        }
        validateInternalTopicReplicationFactor(this.internalTopicReplicationFactor, this.brokersNum);

        if (this.proxyContainer != null) {
            this.proxyContainer.setNetwork(this.network);
            configureProxyContainerPorts();
        }

        prepareKafkaCluster(this.additionalKafkaConfiguration, this.kafkaVersion);
    }

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

        if (this.useDedicatedRoles) {
            prepareDedicatedRolesCluster(defaultKafkaConfigurationForMultiNode, kafkaVersion);
        } else {
            prepareCombinedRolesCluster(defaultKafkaConfigurationForMultiNode, kafkaVersion);
        }
    }

    private void prepareCombinedRolesCluster(final Map<String, String> kafkaConfiguration, final String kafkaVersion) {
        // multi-node set up with combined roles
        this.nodes = IntStream
            .range(0, this.brokersNum)
            .mapToObj(nodeId -> {
                LOGGER.info("Starting combined-role node with id {}", nodeId);
                // adding node id for each kafka container
                StrimziKafkaContainer kafkaContainer = new StrimziKafkaContainer()
                    .withNodeId(nodeId)
                    .withKafkaConfigurationMap(kafkaConfiguration)
                    .withNetwork(this.network)
                    .withProxyContainer(proxyContainer)
                    .withKafkaVersion(kafkaVersion == null ? KafkaVersionService.getInstance().latestRelease().getVersion() : kafkaVersion)
                    // pass shared `cluster.id` to each broker
                    .withClusterId(this.clusterId)
                    .withNodeRole(KafkaNodeRole.COMBINED)
                    .waitForRunning();

                if (this.logFilePath != null) {
                    kafkaContainer.withLogCollection(this.logFilePath);
                }

                if (this.fixedExposedPort > 0) {
                    kafkaContainer.withPort(this.fixedExposedPort + nodeId);
                }

                if (this.bootstrapServersProvider != null) {
                    kafkaContainer.withBootstrapServers(this.bootstrapServersProvider);
                }

                LOGGER.info("Started combined role node with id: {}", kafkaContainer);

                return kafkaContainer;
            })
            .collect(Collectors.toList());
    }

    private void prepareDedicatedRolesCluster(final Map<String, String> kafkaConfiguration, final String kafkaVersion) {
        // Create controller nodes - they get the first set of IDs
        this.controllers = IntStream
            .range(0, this.controllersNum)
            .mapToObj(controllerId -> {
                LOGGER.info("Starting controller-only node with id {}", controllerId);
                StrimziKafkaContainer controllerContainer = new StrimziKafkaContainer()
                    .withNodeId(controllerId)
                    .withKafkaConfigurationMap(kafkaConfiguration)
                    .withNetwork(this.network)
                    .withProxyContainer(proxyContainer)
                    .withKafkaVersion(kafkaVersion == null ? KafkaVersionService.getInstance().latestRelease().getVersion() : kafkaVersion)
                    .withClusterId(this.clusterId)
                    .withNodeRole(KafkaNodeRole.CONTROLLER)
                    .waitForRunning();

                if (this.logFilePath != null) {
                    controllerContainer.withLogCollection(this.logFilePath);
                }

                LOGGER.info("Started controller-only node with id: {}", controllerContainer);
                return controllerContainer;
            })
            .collect(Collectors.toList());

        // Create broker nodes - use node IDs that don't conflict with controllers
        this.brokers = IntStream
            .range(0, this.brokersNum)
            .mapToObj(brokerIndex -> {
                // Use node IDs that start after the highest controller ID to avoid conflicts
                int nodeId = this.controllersNum + brokerIndex;

                LOGGER.info("Starting broker-only node with node.id={}", nodeId);

                StrimziKafkaContainer brokerContainer = new StrimziKafkaContainer()
                    .withNodeId(nodeId)
                    .withKafkaConfigurationMap(kafkaConfiguration)
                    .withNetwork(this.network)
                    .withProxyContainer(proxyContainer)
                    .withKafkaVersion(kafkaVersion == null ? KafkaVersionService.getInstance().latestRelease().getVersion() : kafkaVersion)
                    .withClusterId(this.clusterId)
                    .withNodeRole(KafkaNodeRole.BROKER)
                    .waitForRunning();

                if (this.logFilePath != null) {
                    brokerContainer.withLogCollection(this.logFilePath);
                }

                if (this.fixedExposedPort > 0) {
                    brokerContainer.withPort(this.fixedExposedPort + brokerIndex);
                }

                if (this.bootstrapServersProvider != null) {
                    brokerContainer.withBootstrapServers(this.bootstrapServersProvider);
                }

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

    @DoNotMutate
    private void configureProxyContainerPorts() {
        if (this.useDedicatedRoles) {
            for (int i = 0; i < this.controllersNum + this.brokersNum; i++) {
                this.proxyContainer.addExposedPort(StrimziKafkaContainer.TOXIPROXY_PORT_BASE + i);
            }
        } else {
            for (int i = 0; i < this.brokersNum; i++) {
                this.proxyContainer.addExposedPort(StrimziKafkaContainer.TOXIPROXY_PORT_BASE + i);
            }
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
        private int controllersNum;
        private boolean useDedicatedRoles;
        private int internalTopicReplicationFactor;
        private Map<String, String> additionalKafkaConfiguration = new HashMap<>();
        private ToxiproxyContainer proxyContainer;
        private boolean enableSharedNetwork;
        private String kafkaVersion;
        private String clusterId;
        private String logFilePath;
        private int fixedExposedPort;
        private Function<StrimziKafkaContainer, String> bootstrapServersProvider;

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
         * Configures the cluster to use dedicated controller and broker nodes instead of combined-role nodes.
         * When enabled, you must also specify the number of controllers using {@link #withNumberOfControllers(int)}.
         *
         * @return the current instance of {@code StrimziKafkaClusterBuilder} for method chaining
         */
        public StrimziKafkaClusterBuilder withDedicatedRoles() {
            this.useDedicatedRoles = true;
            return this;
        }

        /**
         * Sets the number of dedicated controller nodes when using combined roles.
         * This method should be used in conjunction with {@link #withDedicatedRoles()}.
         *
         * @param controllersNum the number of dedicated controller nodes
         * @return the current instance of {@code StrimziKafkaClusterBuilder} for method chaining
         */
        public StrimziKafkaClusterBuilder withNumberOfControllers(int controllersNum) {
            this.controllersNum = controllersNum;
            return this;
        }

        /**
         * Fluent method to enable log collection with a default log file path.
         * The actual filename is determined at runtime when logs are collected.
         *
         * @return StrimziKafkaContainer instance for method chaining
         */
        public StrimziKafkaClusterBuilder withLogCollection() {
            this.logFilePath = "target/strimzi-test-container-logs/";
            return this;
        }

        /**
         * Fluent method to enable log collection for all containers in the cluster with a custom log file path.
         *
         * If the path ends with "/", role-based filenames are automatically appended at runtime for each container:
         *      Controller-only: "kafka-controller-{nodeId}.log"
         *      Broker-only: "kafka-broker-{nodeId}.log"
         *      Combined: "kafka-container-{nodeId}.log"
         * otherwise,  the path doesn't end with "/", it's used as the exact filename base for all containers
         *
         * @param logFilePath the base path where container logs will be saved. Use "/" suffix for automatic role-based naming.
         * @return the current instance of {@code StrimziKafkaClusterBuilder} for method chaining
         */
        public StrimziKafkaClusterBuilder withLogCollection(final String logFilePath) {
            if (logFilePath != null && !logFilePath.trim().isEmpty()) {
                this.logFilePath = logFilePath.trim();
            } else {
                throw new IllegalArgumentException("Log file path cannot be null or empty.");
            }
            return this;
        }

        /**
         * Sets a fixed exposed port for all broker nodes in the cluster.
         * <p>
         * Note: When using multiple brokers, each broker needs a unique port.
         * The first broker will use the specified port, and subsequent brokers will use
         * incrementing port numbers (e.g., if port 9092 is specified for a 3-broker cluster,
         * the brokers will use ports 9092, 9093, and 9094).
         * </p>
         *
         * @param fixedPort the base fixed port to expose for the first broker
         * @return the current instance of {@code StrimziKafkaClusterBuilder} for method chaining
         * @throws IllegalArgumentException if the port is less than or equal to 0
         */
        public StrimziKafkaClusterBuilder withPort(final int fixedPort) {
            if (fixedPort <= 0) {
                throw new IllegalArgumentException("The fixed Kafka port must be greater than 0");
            }
            this.fixedExposedPort = fixedPort;
            return this;
        }

        /**
         * Assigns a provider function for overriding the bootstrap servers string for all broker nodes.
         * <p>
         * This is useful when you need to customize how the bootstrap servers address is generated,
         * for example when using a shared network with specific hostnames.
         * </p>
         *
         * @param provider a function that takes a {@link StrimziKafkaContainer} and returns the bootstrap servers string
         * @return the current instance of {@code StrimziKafkaClusterBuilder} for method chaining
         */
        public StrimziKafkaClusterBuilder withBootstrapServers(final Function<StrimziKafkaContainer, String> provider) {
            this.bootstrapServersProvider = provider;
            return this;
        }

        /**
         * Builds and returns a {@code StrimziKafkaCluster} instance based on the provided configurations.
         *
         * @return a new instance of {@code StrimziKafkaCluster}
         */
        public StrimziKafkaCluster build() {
            // Generate a single cluster ID, which will be shared by all nodes
            this.clusterId = UUID.randomUUID().toString();

            return new StrimziKafkaCluster(this);
        }
    }

    /**
     * Returns the underlying GenericContainer instances for all Kafka nodes in the cluster.
     * In the current setup, all nodes are combined-role (i.e., each acts as both broker and controller) in KRaft mode.
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

    /**
     * Get the bootstrap controllers that can be used for controller operations
     * @return a comma separated list of Kafka controller endpoints
     */
    @Override
    public String getBootstrapControllers() {
        return getControllers().stream()
                .map(KafkaContainer::getBootstrapControllers)
                .collect(Collectors.joining(","));
    }

    /**
     * Get the bootstrap controllers that containers on the same network should use to connect to controllers
     * @return a comma separated list of Kafka controller endpoints
     */
    @DoNotMutate
    public String getNetworkBootstrapControllers() {
        return getControllers().stream()
                .map(controller -> ((StrimziKafkaContainer) controller).getNetworkBootstrapControllers())
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

    private void configureQuorumVoters(final Map<String, String> additionalKafkaConfiguration) {
        final String quorumVoters;
        
        if (this.useDedicatedRoles) {
            // For dedicated roles, only controllers participate in the quorum
            quorumVoters = IntStream.range(0, this.controllersNum)
                .mapToObj(controllerId -> String.format("%d@" + StrimziKafkaContainer.NETWORK_ALIAS_PREFIX + "%d:" + StrimziKafkaContainer.CONTROLLER_PORT, controllerId, controllerId))
                .collect(Collectors.joining(","));
        } else {
            // For combined roles, all nodes participate in the quorum
            quorumVoters = IntStream.range(0, this.brokersNum)
                .mapToObj(nodeId -> String.format("%d@" + StrimziKafkaContainer.NETWORK_ALIAS_PREFIX + "%d:" + StrimziKafkaContainer.CONTROLLER_PORT, nodeId, nodeId))
                .collect(Collectors.joining(","));
        }

        additionalKafkaConfiguration.put("controller.quorum.voters", quorumVoters);
    }

    @Override
    @DoNotMutate
    public void start() {
        // Start proxy container first if configured
        if (this.proxyContainer != null && !this.proxyContainer.isRunning()) {
            this.proxyContainer.start();
        }

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

    @DoNotMutate
    private boolean checkAllBrokersReady() {
        try {
            // check broker nodes for quorum readiness (if combined-node then we check all nodes)
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

    @DoNotMutate
    private boolean isBrokerReady(StrimziKafkaContainer kafkaContainer) throws IOException, InterruptedException {
        Container.ExecResult result = kafkaContainer.execInContainer(
            "bash", "-c",
            "bin/kafka-metadata-quorum.sh --bootstrap-server localhost:" + StrimziKafkaContainer.INTER_BROKER_LISTENER_PORT + " describe --status"
        );
        String output = result.getStdout();

        LOGGER.info("Metadata quorum status from broker {}: {}", kafkaContainer.getNodeId(), output);

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
    /* test */ ToxiproxyContainer getToxiproxyContainer() {
        return proxyContainer;
    }

    /**
     * Returns the controller nodes.
     * For combined-role clusters, this returns all nodes.
     * For dedicated-role clusters, this returns only the controller-only nodes.
     *
     * @return Collection of controller nodes
     */
    public Collection<KafkaContainer> getControllers() {
        if (this.useDedicatedRoles) {
            return this.controllers;
        } else {
            return this.nodes;
        }
    }

    /**
     * Returns the broker nodes.
     * For combined-role clusters, this returns all nodes.
     * For dedicated-role clusters, this returns only the broker-only nodes.
     *
     * Keep the method name getBrokers() to preserve backwards compatibility.
     *
     * @return Collection of broker nodes
     */
    public Collection<KafkaContainer> getBrokers() {
        if (this.useDedicatedRoles) {
            return this.brokers;
        } else {
            return this.nodes;
        }
    }

    /**
     * Checks if the cluster is using dedicated controller/broker roles.
     *
     * @return true if using dedicated roles, false if using combined roles
     */
    public boolean isUsingDedicatedRoles() {
        return this.useDedicatedRoles;
    }

    /**
     * Retrieves the Proxy instance for a specific node by its node ID.
     * Works for both brokers and controllers.
     *
     * @param nodeId the ID of the node for which to retrieve the proxy
     * @return the Proxy instance for the specified node
     * @throws IllegalArgumentException if the node ID is not found in the cluster
     * @throws IllegalStateException if the proxy container has not been configured for the cluster
     */
    @DoNotMutate
    public Proxy getProxyForNode(int nodeId) {
        if (this.proxyContainer == null) {
            throw new IllegalStateException("Proxy container has not been configured for this cluster");
        }

        for (final KafkaContainer node : this.nodes) {
            final StrimziKafkaContainer kafkaNode = (StrimziKafkaContainer) node;
            if (kafkaNode.getNodeId() == nodeId) {
                return kafkaNode.getProxy();
            }
        }

        throw new IllegalArgumentException("Node with ID " + nodeId + " not found in cluster");
    }
}
