/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.lifecycle.Startables;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * A multi-node instance of Kafka and Zookeeper using the latest image from quay.io/strimzi/kafka with the given version.
 * It perfectly fits for integration/system testing. We always deploy one zookeeper with a specified number of Kafka instances,
 * running as a separate container inside Docker. The additional configuration for Kafka brokers can be passed to the constructor.
 * <br><br>
 * Note: Direct constructor calls are deprecated and will be removed in the next released version.
 * Please use {@link StrimziKafkaClusterBuilder} for creating instances of this class.
 */
public class StrimziKafkaCluster implements KafkaContainer {

    // class attributes
    private static final Logger LOGGER = LoggerFactory.getLogger(StrimziKafkaCluster.class);

    // instance attributes
    private int brokersNum;
    private int internalTopicReplicationFactor;
    private Map<String, String> additionalKafkaConfiguration;
    private ToxiproxyContainer proxyContainer;
    private boolean enableSharedNetwork;
    private String kafkaVersion;

    // not editable
    private final Network network;
    private final StrimziZookeeperContainer zookeeper;
    private Collection<KafkaContainer> brokers;

    /**
     * Constructor for @StrimziKafkaCluster class, which allows you to specify number of brokers @see{brokersNum},
     * replication factor of internal topics @see{internalTopicReplicationFactor}, a map of additional Kafka
     * configuration @see{additionalKafkaConfiguration} and a {@code proxyContainer}.
     * <br><br>
     * The {@code proxyContainer} allows to simulate network conditions (i.e. connection cut, latency).
     * For example, you can simulate a network partition by cutting the connection of one or more brokers.
     *
     * @param brokersNum number of brokers
     * @param internalTopicReplicationFactor internal topics
     * @param additionalKafkaConfiguration additional Kafka configuration
     * @param proxyContainer Proxy container
     * @param enableSharedNetwork enable Kafka cluster to use a shared Docker network.
     */
    @Deprecated
    public StrimziKafkaCluster(final int brokersNum,
                               final int internalTopicReplicationFactor,
                               final Map<String, String> additionalKafkaConfiguration,
                               final ToxiproxyContainer proxyContainer,
                               final boolean enableSharedNetwork) {
        validateBrokerNum(brokersNum);
        validateInternalTopicReplicationFactor(internalTopicReplicationFactor, brokersNum);

        this.brokersNum = brokersNum;
        this.network = enableSharedNetwork ? Network.SHARED : Network.newNetwork();
        this.zookeeper = new StrimziZookeeperContainer()
            .withNetwork(this.network);
        this.internalTopicReplicationFactor = internalTopicReplicationFactor;

        if (proxyContainer != null) {
            proxyContainer.setNetwork(this.network);
        }

        prepareKafkaCluster(additionalKafkaConfiguration, null);
    }

    /**
     * Constructor for @StrimziKafkaCluster class, which allows you to specify number of brokers @see{brokersNum},
     * replication factor of internal topics @see{internalTopicReplicationFactor} and map of additional Kafka
     * configuration @see{additionalKafkaConfiguration}.
     *
     * @param brokersNum number of brokers
     * @param internalTopicReplicationFactor internal topics
     * @param additionalKafkaConfiguration additional Kafka configuration
     */
    @Deprecated
    public StrimziKafkaCluster(final int brokersNum,
                               final int internalTopicReplicationFactor,
                               final Map<String, String> additionalKafkaConfiguration) {
        this(brokersNum, internalTopicReplicationFactor, additionalKafkaConfiguration, null, false);
    }

    /**
     * Constructor of StrimziKafkaCluster without specifying additional configuration.
     *
     * @param brokersNum number of brokers
     */
    @Deprecated
    public StrimziKafkaCluster(final int brokersNum) {
        this(brokersNum, brokersNum, null, null, false);
    }

    /**
     * Constructor of StrimziKafkaCluster with proxy container
     *
     * @param brokersNum number of brokers to be deployed
     * @param proxyContainer Proxy container
     */
    @Deprecated
    public StrimziKafkaCluster(final int brokersNum, final ToxiproxyContainer proxyContainer) {
        this(brokersNum, brokersNum, null, proxyContainer, false);
    }

    /**
     * Constructor of StrimziKafkaCluster with proxy container
     *
     * @param brokersNum number of brokers to be deployed
     * @param enableSharedNetwork enable Kafka cluster to use a shared Docker network.
     */
    @Deprecated
    public StrimziKafkaCluster(final int brokersNum, final boolean enableSharedNetwork) {
        this(brokersNum, brokersNum, null, null, enableSharedNetwork);
    }

    private StrimziKafkaCluster(StrimziKafkaClusterBuilder builder) {
        this.brokersNum = builder.brokersNum;
        this.enableSharedNetwork = builder.enableSharedNetwork;
        this.network = this.enableSharedNetwork ? Network.SHARED : Network.newNetwork();
        this.internalTopicReplicationFactor = builder.internalTopicReplicationFactor == 0 ? this.brokersNum : builder.internalTopicReplicationFactor;
        this.additionalKafkaConfiguration = builder.additionalKafkaConfiguration;
        this.proxyContainer = builder.proxyContainer;
        this.kafkaVersion = builder.kafkaVersion;

        validateBrokerNum(this.brokersNum);
        validateInternalTopicReplicationFactor(this.internalTopicReplicationFactor, this.brokersNum);

        this.zookeeper = new StrimziZookeeperContainer()
            .withNetwork(this.network);

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

        if (additionalKafkaConfiguration != null) {
            defaultKafkaConfigurationForMultiNode.putAll(additionalKafkaConfiguration);
        }

        // multi-node set up
        this.brokers = IntStream
            .range(0, this.brokersNum)
            .mapToObj(brokerId -> {
                LOGGER.info("Starting broker with id {}", brokerId);
                // adding broker id for each kafka container
                KafkaContainer kafkaContainer = new StrimziKafkaContainer()
                    .withBrokerId(brokerId)
                    .withKafkaConfigurationMap(defaultKafkaConfigurationForMultiNode)
                    .withExternalZookeeperConnect("zookeeper:" + StrimziZookeeperContainer.ZOOKEEPER_PORT)
                    .withNetwork(this.network)
                    .withProxyContainer(proxyContainer)
                    .withNetworkAliases("broker-" + brokerId)
                    .withKafkaVersion(kafkaVersion == null ? KafkaVersionService.getInstance().latestRelease().getVersion() : kafkaVersion)
                    .dependsOn(this.zookeeper);

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
        if (internalTopicReplicationFactor <= 0 || internalTopicReplicationFactor > brokersNum) {
            throw new IllegalArgumentException("internalTopicReplicationFactor '" + internalTopicReplicationFactor + "' must be less than brokersNum and greater than 0");
        }
    }

    public static class StrimziKafkaClusterBuilder {
        private int brokersNum;
        private int internalTopicReplicationFactor;
        private Map<String, String> additionalKafkaConfiguration = new HashMap<>();
        private ToxiproxyContainer proxyContainer;
        private boolean enableSharedNetwork;
        private String kafkaVersion;

        /**
         * Sets the number of Kafka brokers in the cluster.
         *
         * @param brokersNum the number of Kafka brokers
         * @return the current instance of {@code StrimziKafkaClusterBuilder} for method chaining
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
         * @return the current instance of {@code StrimziKafkaClusterBuilder} for method chaining
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
         * @return the current instance of {@code StrimziKafkaClusterBuilder} for method chaining
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
         * @return the current instance of {@code StrimziKafkaClusterBuilder} for method chaining
         */
        public StrimziKafkaClusterBuilder withProxyContainer(ToxiproxyContainer proxyContainer) {
            this.proxyContainer = proxyContainer;
            return this;
        }

        /**
         * Enables a shared Docker network for the Kafka cluster.
         * This allows the Kafka cluster to interact with other containers on the same network.
         *
         * @return the current instance of {@code StrimziKafkaClusterBuilder} for method chaining
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
         * @return the current instance of {@code StrimziKafkaClusterBuilder} for method chaining
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
            return new StrimziKafkaCluster(this);
        }
    }

    /**
     * Get collection of Strimzi kafka containers
     * @return collection of Strimzi kafka containers
     */
    public Collection<KafkaContainer> getBrokers() {
        return this.brokers;
    }

    @Override
    public boolean hasKraftOrExternalZooKeeperConfigured() {
        KafkaContainer broker0 = brokers.iterator().next();
        return broker0.hasKraftOrExternalZooKeeperConfigured() ? true : false;
    }

    @Override
    public String getInternalZooKeeperConnect() {
        if (hasKraftOrExternalZooKeeperConfigured()) {
            throw new IllegalStateException("Connect string is not available when using KRaft or external ZooKeeper");
        }
        return getZookeeper() != null ? getZookeeper().getConnectString() : null;
    }

    @Override
    public String getBootstrapServers() {
        return brokers.stream()
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

    @Override
    public void start() {
        Stream<KafkaContainer> startables = this.brokers.stream();
        try {
            Startables.deepStart(startables).get(60, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        }

        Utils.waitFor("Kafka brokers nodes to be connected to the ZooKeeper", Duration.ofSeconds(5).toMillis(), Duration.ofMinutes(1).toMillis(),
            () -> {
                Container.ExecResult result;
                try {
                    result = this.zookeeper.execInContainer(
                        "sh", "-c",
                        "bin/zookeeper-shell.sh zookeeper:" + StrimziZookeeperContainer.ZOOKEEPER_PORT + " ls /brokers/ids | tail -n 1"
                    );
                    String brokers = result.getStdout();

                    LOGGER.info("Running Kafka brokers: {}", result.getStdout());

                    return brokers != null && brokers.split(",").length == this.brokersNum;
                } catch (IOException | InterruptedException e) {
                    Thread.currentThread().interrupt();
                    e.printStackTrace();
                    return false;
                }
            });
    }

    @Override
    public void stop() {
        // firstly we shut-down zookeeper -> reason: 'On the command line if I kill ZK first it sometimes prevents a broker from shutting down quickly.'
        this.zookeeper.stop();

        // stop all kafka containers in parallel
        this.brokers.stream()
            .parallel()
            .forEach(KafkaContainer::stop);
    }

    /**
     * Get @code{StrimziZookeeperContainer} instance
     * @return StrimziZookeeperContainer instance
     */
    public StrimziZookeeperContainer getZookeeper() {
        return zookeeper;
    }
}
