/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ContainerNetwork;
import io.strimzi.utils.Environment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.images.builder.Transferable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * StrimziKafkaContainer is a single-node instance of Kafka using the image from quay.io/strimzi/kafka with the
 * given version. There are two options for how to use it. The first one is using an embedded zookeeper which will run
 * inside Kafka container. The Another option is to use @StrimziZookeeperContainer as an external Zookeeper.
 * The additional configuration for Kafka broker can be injected via constructor. This container is a good fit for
 * integration testing but for more hardcore testing we suggest using @StrimziKafkaCluster.
 */
public class StrimziKafkaContainer extends GenericContainer<StrimziKafkaContainer> {

    private static final Logger LOGGER = LogManager.getLogger(StrimziKafkaContainer.class);

    /**
     * Default Kafka port
     */
    public static final int KAFKA_PORT = 9092;
    /**
     * Default ZooKeeper port
     */
    public static final int ZOOKEEPER_PORT = 2181;

    private static final String STARTER_SCRIPT = "/testcontainers_start.sh";

    private static final String DEFAUT_STORAGE_UUID = "xtzWWN4bTjitpL4efd9s6g";

    private Map<String, String> kafkaConfigurationMap;
    private String externalZookeeperConnect;
    private int kafkaExposedPort;
    private final int brokerId;
    private final boolean useKraft;
    private String storageUUID = DEFAUT_STORAGE_UUID;

    private StrimziKafkaContainer(final int brokerId, final boolean useKraft, final Map<String, String> additionalKafkaConfiguration) {
        super("quay.io/strimzi-test-container/test-container:" +
            Environment.getValue(Environment.STRIMZI_TEST_CONTAINER_IMAGE_VERSION_ENV) + "-kafka-" +
            Environment.getValue(Environment.STRIMZI_TEST_CONTAINER_KAFKA_VERSION_ENV));
        super.withNetwork(Network.SHARED);
        super.withExposedPorts(KAFKA_PORT);
        super.withEnv("LOG_DIR", "/tmp");

        this.useKraft = useKraft;
        this.brokerId = brokerId;
        this.kafkaConfigurationMap = new HashMap<>(additionalKafkaConfiguration);
        this.kafkaConfigurationMap.put("broker.id", String.valueOf(this.brokerId));
    }

    /**
     * Create @code{StrimziKafkaContainer} instance with external ZooKeeper (i.e., @code{StrimziZooKeeperContainer}).
     *
     * Creating the container using this method disables Kraft.
     *
     * @param brokerId broker id
     * @param connectString connect string
     * @param additionalKafkaConfiguration addtional configuration
     * @return instance of @code{StrimziKafkaContainer}
     */
    public static StrimziKafkaContainer createWithExternalZookeeper(final int brokerId,
                                                                    final String connectString, final Map<String, String> additionalKafkaConfiguration) {
        return new StrimziKafkaContainer(brokerId, false, additionalKafkaConfiguration)
            .withExternalZookeeper(connectString);
    }

    /**
     * Static factory method, which creates @code{StrimziKafkaContainer} with additional configuration of Kafka broker
     * @param brokerId broker id
     * @param useKraft whether the broker should use Kraft
     * @param additionalKafkaConfiguration additional configuration for Kafka broker
     * @return instance of @code{StrimziKafkaContainer}
     */
    public static StrimziKafkaContainer createWithAdditionalConfiguration(final int brokerId, final boolean useKraft, final Map<String, String> additionalKafkaConfiguration) {
        return new StrimziKafkaContainer(brokerId, useKraft, additionalKafkaConfiguration);
    }

    /**
     * Static factory method, which creates @code{StrimziKafkaContainer} with additional configuration of Kafka broker
     * @param brokerId broker id
     * @param additionalKafkaConfiguration additional configuration for Kafka broker
     * @return instance of @code{StrimziKafkaContainer}
     */
    public static StrimziKafkaContainer createWithAdditionalConfiguration(final int brokerId, final Map<String, String> additionalKafkaConfiguration) {
        return createWithAdditionalConfiguration(brokerId, false, additionalKafkaConfiguration);
    }

    /**
     * Static factory method, which creates @code{StrimziKafkaContainer} with empty additional configuration of Kafka broker
     * @param brokerId broker id
     * @return instance of StrimziKafkaContainer
     */
    public static StrimziKafkaContainer create(final int brokerId) {
        return new StrimziKafkaContainer(brokerId, false, Collections.emptyMap());
    }

    /**
     * Static factory method, which creates @code{StrimziKafkaContainer} with empty additional configuration of Kafka broker, and use Kraft.
     * @param brokerId broker id
     * @return instance of StrimziKafkaContainer
     */
    public static StrimziKafkaContainer createWithKraft(final int brokerId) {
        return new StrimziKafkaContainer(brokerId, true, Collections.emptyMap());
    }

    @Override
    protected void doStart() {
        // we need it for the startZookeeper(); and startKafka(); to run container before...
        withCommand("sh", "-c", "while [ ! -f " + STARTER_SCRIPT + " ]; do sleep 0.1; done; " + STARTER_SCRIPT);
        super.doStart();
    }

    /**
     * Fluent method, which sets @code{externalZookeeperConnect}.
     *
     * If the broker was created using Kraft, this method throws an {@link IllegalArgumentException}.
     *
     * @param connectString connect string
     * @return StrimziKafkaContainer instance
     */
    public StrimziKafkaContainer withExternalZookeeper(final String connectString) {
        if (useKraft) {
            throw new IllegalArgumentException("Cannot configure an external Zookeeper and use Kraft at the same time");
        }
        this.externalZookeeperConnect = connectString;
        return self();
    }

    /**
     * Fluent method, which sets a waiting strategy to wait until the broker is ready.
     *
     * This method waits for a log message in the broker log.
     * You can customize the strategy using {@link #waitingFor(WaitStrategy)}.
     *
     * @return StrimziKafkaContainer instance
     */
    public StrimziKafkaContainer waitForRunning() {
        super.waitingFor(Wait.forLogMessage(".*Transitioning from RECOVERY to RUNNING.*", 1));
        return self();
    }

    /**
     * Fluent method, which sets @code{storageUUID}.
     * The storage UUID must be the 16 bytes of a base64-encoded UUID.
     *
     * This method requires the broker to be created using Kraft.
     *
     * @param uuid the uuid, must not be {@code null} or blank.
     * @return StrimziKafkaContainer instance
     */
    public StrimziKafkaContainer withStorageUUID(final String uuid) {
        if (!useKraft) {
            throw new IllegalArgumentException("Setting the storage UUID requires Kraft");
        }
        if (uuid == null || uuid.trim().isEmpty()) {
            throw new IllegalArgumentException("The UUID must not be blank");
        }
        this.storageUUID = uuid;
        return self();
    }

    @Override
    protected void containerIsStarting(final InspectContainerResponse containerInfo, final boolean reused) {
        super.containerIsStarting(containerInfo, reused);

        this.kafkaExposedPort = getMappedPort(KAFKA_PORT);

        LOGGER.info("Mapped port: {}", this.kafkaExposedPort);

        StringBuilder advertisedListeners = new StringBuilder(getBootstrapServers());

        Collection<ContainerNetwork> cns = containerInfo.getNetworkSettings().getNetworks().values();

        int advertisedListenerNumber = 1;
        List<String> advertisedListenersNames = new ArrayList<>();

        for (ContainerNetwork cn : cns) {
            // must be always unique
            final String advertisedName = "BROKER" + advertisedListenerNumber;
            advertisedListeners.append(",").append(advertisedName).append("://").append(cn.getIpAddress()).append(":9093");
            advertisedListenersNames.add(advertisedName);
            advertisedListenerNumber++;
        }

        LOGGER.info("This is all advertised listeners for Kafka {}", advertisedListeners.toString());

        StringBuilder kafkaListeners = new StringBuilder();
        StringBuilder kafkaListenerSecurityProtocol = new StringBuilder();

        advertisedListenersNames.forEach(name -> {
            // listeners
            kafkaListeners
                .append(name)
                .append("://0.0.0.0:9093")
                .append(",");
            // listener.security.protocol.map
            kafkaListenerSecurityProtocol
                .append(name)
                .append(":PLAINTEXT")
                .append(",");
        });

        StringBuilder kafkaConfiguration = new StringBuilder();

        // Common configuration
        kafkaConfiguration
                .append(" --override listeners=").append(kafkaListeners).append("PLAINTEXT://0.0.0.0:").append(KAFKA_PORT)
                .append(" --override advertised.listeners=").append(advertisedListeners)
                .append(" --override listener.security.protocol.map=").append(kafkaListenerSecurityProtocol).append("PLAINTEXT:PLAINTEXT")
                .append(" --override inter.broker.listener.name=BROKER1");

        if (useKraft) {
            kafkaConfiguration
                    .append(" --override controller.listener.names=").append("BROKER1");
        } else {
            kafkaConfiguration
                    .append(" --override zookeeper.connect=localhost:").append(ZOOKEEPER_PORT);
        }

        // additional kafka config
        this.kafkaConfigurationMap.forEach((configName, configValue) ->
            kafkaConfiguration
                .append(" --override ")
                .append(configName)
                .append("=")
                .append(configValue));

        String command = "#!/bin/bash \n";

        if (!useKraft) {
            if (this.externalZookeeperConnect != null) {
                withEnv("KAFKA_ZOOKEEPER_CONNECT", this.externalZookeeperConnect);
            } else {
                command += "bin/zookeeper-server-start.sh config/zookeeper.properties &\n";
            }
            command += "bin/kafka-server-start.sh config/server.properties" + kafkaConfiguration;
        } else {
            command += "bin/kafka-storage.sh format -t " + storageUUID + " -c config/kraft/server.properties \n";
            command += "bin/kafka-server-start.sh config/kraft/server.properties" + kafkaConfiguration;
        }

        LOGGER.info("Copying command to 'STARTER_SCRIPT' script.");

        copyFileToContainer(
            Transferable.of(command.getBytes(StandardCharsets.UTF_8), 700),
            STARTER_SCRIPT
        );
    }

    /**
     * Get bootstrap servers of @code{StrimziKafkaContainer} instance
     * @return bootstrap servers
     */
    public String getBootstrapServers() {
        return String.format("PLAINTEXT://%s:%s", getContainerIpAddress(), this.kafkaExposedPort);
    }
}
