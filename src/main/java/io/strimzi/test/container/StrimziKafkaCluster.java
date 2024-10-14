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

    // not editable
    private final Network network;
    private final StrimziZookeeperContainer zookeeper;
    private final Collection<KafkaContainer> brokers;

    private StrimziKafkaCluster(StrimziKafkaClusterBuilder builder) {
        this.brokersNum = builder.brokersNum;
        this.enableSharedNetwork = builder.enableSharedNetwork;
        this.network = this.enableSharedNetwork ? Network.SHARED : Network.newNetwork();
        this.internalTopicReplicationFactor = builder.internalTopicReplicationFactor == 0 ? this.brokersNum : builder.internalTopicReplicationFactor;
        this.additionalKafkaConfiguration = builder.additionalKafkaConfiguration;
        this.proxyContainer = builder.proxyContainer;

        if (this.brokersNum <= 0) {
            throw new IllegalArgumentException("brokersNum '" + this.brokersNum + "' must be greater than 0");
        }
        if (this.internalTopicReplicationFactor <= 0 || this.internalTopicReplicationFactor > this.brokersNum) {
            throw new IllegalArgumentException("internalTopicReplicationFactor '" + this.internalTopicReplicationFactor + "' must be less than brokersNum and greater than 0");
        }

        this.zookeeper = new StrimziZookeeperContainer()
            .withNetwork(this.network);

        final Map<String, String> defaultKafkaConfigurationForMultiNode = new HashMap<>();
        defaultKafkaConfigurationForMultiNode.put("offsets.topic.replication.factor", String.valueOf(internalTopicReplicationFactor));
        defaultKafkaConfigurationForMultiNode.put("num.partitions", String.valueOf(internalTopicReplicationFactor));
        defaultKafkaConfigurationForMultiNode.put("transaction.state.log.replication.factor", String.valueOf(internalTopicReplicationFactor));
        defaultKafkaConfigurationForMultiNode.put("transaction.state.log.min.isr", String.valueOf(internalTopicReplicationFactor));

        if (this.additionalKafkaConfiguration != null) {
            defaultKafkaConfigurationForMultiNode.putAll(this.additionalKafkaConfiguration);
        }

        if (this.proxyContainer != null) {
            this.proxyContainer.setNetwork(this.network);
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
                    .dependsOn(this.zookeeper);

                LOGGER.info("Started broker with id: {}", kafkaContainer);

                return kafkaContainer;
            })
            .collect(Collectors.toList());
    }


    public static class StrimziKafkaClusterBuilder {
        private int brokersNum;
        private int internalTopicReplicationFactor;
        private Map<String, String> additionalKafkaConfiguration = new HashMap<>();
        private ToxiproxyContainer proxyContainer;
        private boolean enableSharedNetwork;

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
