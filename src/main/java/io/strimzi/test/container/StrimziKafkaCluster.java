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
 * A multi-node instance of Kafka and Zookeeper using the latest image from quay.io/strimzi/kafka with the given version.
 * It perfectly fits for integration/system testing. We always deploy one zookeeper with a specified number of Kafka instances,
 * running as a separate container inside Docker. The additional configuration for Kafka brokers can be passed to the constructor.
 * <br><br>
 */
public class StrimziKafkaCluster implements KafkaContainer {

    // class attributes
    private static final Logger LOGGER = LoggerFactory.getLogger(StrimziKafkaCluster.class);

    // instance attributes
    private final int brokersNum;
    private final int internalTopicReplicationFactor;
    private final Map<String, String> additionalKafkaConfiguration;
    private final ToxiproxyContainer proxyContainer;
    private final boolean enableSharedNetwork;
    private final String kafkaVersion;

    // not editable
    private final Network network;
    private Collection<KafkaContainer> nodes;
    private final String clusterId;

    private StrimziKafkaCluster(StrimziKafkaClusterBuilder builder) {
        this.brokersNum = builder.brokersNum;
        this.enableSharedNetwork = builder.enableSharedNetwork;
        this.network = this.enableSharedNetwork ? Network.SHARED : Network.newNetwork();
        this.internalTopicReplicationFactor = builder.internalTopicReplicationFactor == 0 ? this.brokersNum : builder.internalTopicReplicationFactor;
        this.additionalKafkaConfiguration = builder.additionalKafkaConfiguration;
        this.proxyContainer = builder.proxyContainer;
        this.kafkaVersion = builder.kafkaVersion;
        this.clusterId = builder.clusterId;

        validateBrokerNum(this.brokersNum);
        validateInternalTopicReplicationFactor(this.internalTopicReplicationFactor, this.brokersNum);

        if (this.proxyContainer != null) {
            this.proxyContainer.setNetwork(this.network);
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

        // multi-node set up
        this.nodes = IntStream
            .range(0, this.brokersNum)
            .mapToObj(brokerId -> {
                LOGGER.info("Starting broker with id {}", brokerId);
                // adding broker id for each kafka container
                StrimziKafkaContainer kafkaContainer = new StrimziKafkaContainer()
                    .withBrokerId(brokerId)
                    .withKafkaConfigurationMap(defaultKafkaConfigurationForMultiNode)
                    .withNetwork(this.network)
                    .withProxyContainer(proxyContainer)
                    .withKafkaVersion(kafkaVersion == null ? KafkaVersionService.getInstance().latestRelease().getVersion() : kafkaVersion)
                    // One must set `node.id` to the same value as `broker.id` if we use KRaft mode
                    .withNodeId(brokerId)
                    // pass shared `cluster.id` to each broker
                    .withClusterId(this.clusterId)
                    .waitForRunning();

                LOGGER.info("Started broker with id: {}", kafkaContainer);

                return kafkaContainer;
            })
            .collect(Collectors.toList());
    }

    private void validateBrokerNum(int brokersNum) {
        if (brokersNum <= 0) {
            throw new IllegalArgumentException("brokersNum '" + brokersNum + "' must be greater than 0");
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
         * Builds and returns a {@code StrimziKafkaCluster} instance based on the provided configurations.
         *
         * @return a new instance of {@code StrimziKafkaCluster}
         */
        public StrimziKafkaCluster build() {
            // Generate a single cluster ID, which will be shared by all brokers
            this.clusterId = UUID.randomUUID().toString();

            return new StrimziKafkaCluster(this);
        }
    }

    /**
     * Get collection of Strimzi kafka containers
     *
     * @deprecated use {@link #getNodes()} instead.
     * @return collection of Strimzi kafka containers
     */
    @Deprecated
    public Collection<KafkaContainer> getBrokers() {
        return this.nodes;
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
    @DoNotMutate
    public String getNetworkBootstrapServers() {
        return nodes.stream()
                .map(broker -> ((StrimziKafkaContainer) broker).getNetworkBootstrapServers())
                .collect(Collectors.joining(","));
    }

    @Override
    public String getBootstrapServers() {
        return nodes.stream()
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

    private void configureQuorumVoters(final Map<String, String> additionalKafkaConfiguration) {
        // Construct controller.quorum.voters based on network aliases (broker-1, broker-2, etc.)
        final String quorumVoters = IntStream.range(0, this.brokersNum)
            .mapToObj(brokerId -> String.format("%d@" + StrimziKafkaContainer.NETWORK_ALIAS_PREFIX + "%d:9094", brokerId, brokerId))
            .collect(Collectors.joining(","));

        additionalKafkaConfiguration.put("controller.quorum.voters", quorumVoters);
    }

    @SuppressWarnings({"CyclomaticComplexity", "NPathComplexity"})
    @Override
    @DoNotMutate
    public void start() {
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

        // Readiness check for KRaft mode
        Utils.waitFor("Kafka brokers to form a quorum", Duration.ofSeconds(1), Duration.ofMinutes(1),
            () -> {
                try {
                    for (KafkaContainer kafkaContainer : this.nodes) {
                        Container.ExecResult result = ((StrimziKafkaContainer) kafkaContainer).execInContainer(
                            "bash", "-c",
                            "bin/kafka-metadata-quorum.sh --bootstrap-server localhost:" + StrimziKafkaContainer.INTER_BROKER_LISTENER_PORT + " describe --status"
                        );
                        String output = result.getStdout();

                        LOGGER.info("Metadata quorum status from broker {}: {}", ((StrimziKafkaContainer) kafkaContainer).getBrokerId(), output);

                        if (output == null || output.isEmpty()) {
                            return false;
                        }

                        // Check if LeaderId is present and valid
                        final Pattern leaderIdPattern = Pattern.compile("LeaderId:\\s+(\\d+)");
                        final Matcher leaderIdMatcher = leaderIdPattern.matcher(output);

                        if (!leaderIdMatcher.find()) {
                            return false; // LeaderId not found
                        }

                        String leaderIdStr = leaderIdMatcher.group(1);
                        try {
                            int leaderId = Integer.parseInt(leaderIdStr);
                            if (leaderId < 0) {
                                return false; // Invalid LeaderId
                            }
                        } catch (NumberFormatException e) {
                            return false; // LeaderId is not a valid integer
                        }

                        // If LeaderId is present and valid, we assume the broker is ready
                    }
                    return true; // All brokers have a valid LeaderId
                } catch (IOException | InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Failed to execute command in Kafka container", e);
                }
            });
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
}
