/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import io.strimzi.test.container.utils.Constants;
import io.strimzi.test.container.utils.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startable;
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
public class StrimziKafkaCluster implements Startable {

    // class attributes
    private static final Logger LOGGER = LogManager.getLogger(StrimziKafkaCluster.class);

    // instance attributes
    private final int brokersNum;
    private final Network network;
    private final StrimziZookeeperContainer zookeeper;
    private final Collection<StrimziKafkaContainer> brokers;

    /**
     * Constructor for @StrimziKafkaCluster class, which allows you to specify number of brokers @see{brokersNum},
     * replication factor of internal topics @see{internalTopicReplicationFactor} and map of additional Kafka
     * configuration @see{additionalKafkaConfiguration}.
     * @param brokersNum number of brokers
     * @param internalTopicReplicationFactor internal topics
     * @param additionalKafkaConfiguration additional Kafka configuration
     */
    public StrimziKafkaCluster(final int brokersNum, final int internalTopicReplicationFactor, final Map<String, String> additionalKafkaConfiguration) {
        if (brokersNum < 0) {
            throw new IllegalArgumentException("brokersNum '" + brokersNum + "' must be greater than 0");
        }
        if (internalTopicReplicationFactor < 0 || internalTopicReplicationFactor > brokersNum) {
            throw new IllegalArgumentException("internalTopicReplicationFactor '" + internalTopicReplicationFactor + "' must be less than brokersNum and greater than 0");
        }

        this.brokersNum = brokersNum;
        this.network = Network.newNetwork();

        this.zookeeper = new StrimziZookeeperContainer()
            .withNetwork(this.network);

        Map<String, String> defaultKafkaConfigurationForMultiNode = new HashMap<>();
        defaultKafkaConfigurationForMultiNode.put("offsets.topic.replication.factor", String.valueOf(internalTopicReplicationFactor));
        defaultKafkaConfigurationForMultiNode.put("num.partitions", String.valueOf(internalTopicReplicationFactor));
        defaultKafkaConfigurationForMultiNode.put("transaction.state.log.replication.factor", String.valueOf(internalTopicReplicationFactor));
        defaultKafkaConfigurationForMultiNode.put("transaction.state.log.min.isr", String.valueOf(internalTopicReplicationFactor));

        additionalKafkaConfiguration.putAll(defaultKafkaConfigurationForMultiNode);

        // multi-node set up
        this.brokers = IntStream
            .range(0, this.brokersNum)
            .mapToObj(brokerId -> {
                LOGGER.info("Starting broker with id {}", brokerId);
                // adding broker id for each kafka container
                StrimziKafkaContainer kafkaContainer = new StrimziKafkaContainer()
                    .withBrokerId(brokerId)
                    .withKafkaConfigurationMap(additionalKafkaConfiguration)
                    .withExternalZookeeperConnect("zookeeper:" + Constants.ZOOKEEPER_PORT)
                    .withNetwork(this.network)
                    .withNetworkAliases("broker-" + brokerId)
                    .dependsOn(this.zookeeper);

                LOGGER.info("Started broker with id: {}", kafkaContainer);

                return kafkaContainer;
            })
            .collect(Collectors.toList());
    }

    /**
     * Get collection of Strimzi kafka containers
     * @return collection of Strimzi kafka containers
     */
    public Collection<StrimziKafkaContainer> getBrokers() {
        return this.brokers;
    }

    /**
     * Get bootstrap servers as a string, which can be inserted inside Kafka config
     * @return bootstrap servers
     */
    public String getBootstrapServers() {
        return brokers.stream()
            .map(StrimziKafkaContainer::getBootstrapServers)
            .collect(Collectors.joining(","));
    }

    @Override
    public void start() {
        Stream<? extends Startable> startables = this.brokers.stream();
        try {
            Startables.deepStart(startables).get(60, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        }

        Utils.waitFor("Broker node", Duration.ofSeconds(1).toMillis(), Duration.ofSeconds(30).toMillis(),
            () -> {
                Container.ExecResult result;
                try {
                    result = this.zookeeper.execInContainer(
                        "sh", "-c",
                        "bin/zookeeper-shell.sh zookeeper:" + Constants.ZOOKEEPER_PORT + " ls /brokers/ids | tail -n 1"
                    );
                    String brokers = result.getStdout();

                    LOGGER.info("Stdout from zookeeper container....{}", result.getStdout());

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
            .forEach(StrimziKafkaContainer::stop);
    }

    /**
     * Get @code{StrimziZookeeperContainer} instance
     * @return StrimziZookeeperContainer instance
     */
    public StrimziZookeeperContainer getZookeeper() {
        return zookeeper;
    }
}
