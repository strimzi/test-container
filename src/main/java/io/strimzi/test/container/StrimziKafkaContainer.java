/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ContainerNetwork;
import io.strimzi.utils.Constants;
import io.strimzi.utils.KafkaVersionService;
import io.strimzi.utils.Utils;
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

    // class attributes
    private static final Logger LOGGER = LogManager.getLogger(StrimziKafkaContainer.class);
    private static final String STARTER_SCRIPT = "/testcontainers_start.sh";
    private static final KafkaVersionService LOGICAL_KAFKA_VERSION_ENTITY;

    // instance attributes
    private int kafkaDynamicKafkaPort;
    private Map<String, String> kafkaConfigurationMap;
    private String externalZookeeperConnect;
    private int brokerId;
    private String kafkaVersion;
    private String strimziTestContainerImageVersion;
    private boolean useKraft;
    private String storageUUID;

    static {
        LOGICAL_KAFKA_VERSION_ENTITY = new KafkaVersionService();
    }

    private void buildDefaults() {
        String strimziTestContainerVersion;
        if (this.strimziTestContainerImageVersion == null || this.strimziTestContainerImageVersion.isEmpty()) {
            strimziTestContainerVersion = LOGICAL_KAFKA_VERSION_ENTITY.latestRelease().getStrimziTestContainerVersion();
            LOGGER.info("No Strimzi test container version specified. Using latest release:{}", strimziTestContainerVersion);
        } else {
            strimziTestContainerVersion = this.strimziTestContainerImageVersion;
        }
        String kafkaVersion;
        if (this.kafkaVersion == null || this.kafkaVersion.isEmpty()) {
            kafkaVersion = LOGICAL_KAFKA_VERSION_ENTITY.latestRelease().getVersion();
            LOGGER.info("No Kafka version specified. Using latest release:{}", kafkaVersion);
        } else {
            kafkaVersion = this.kafkaVersion;
        }
        this.storageUUID = this.storageUUID == null ? Utils.randomUuid().toString() : this.storageUUID;

        // we need this shared network in case we deploy StrimziKafkaCluster which consist of `StrimziKafkaContainer`
        // instances and by default each container has its own network, which results in `Unable to resolve address: zookeeper:2181`
        super.setNetwork(Network.SHARED);
        // exposing kafka port from the container
        super.setExposedPorts(Collections.singletonList(Constants.KAFKA_PORT));
        super.addEnv("LOG_DIR", "/tmp");
        super.setDockerImageName("quay.io/strimzi-test-container/test-container:" +
            strimziTestContainerVersion + "-kafka-" +
            kafkaVersion);
    }

    @Override
    protected void doStart() {
        buildDefaults();
        // we need it for the startZookeeper(); and startKafka(); to run container before...
        super.setCommand("sh", "-c", "while [ ! -f " + STARTER_SCRIPT + " ]; do sleep 0.1; done; " + STARTER_SCRIPT);
        super.doStart();
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
        return this;
    }

    @Override
    protected void containerIsStarting(final InspectContainerResponse containerInfo, final boolean reused) {
        super.containerIsStarting(containerInfo, reused);

        kafkaDynamicKafkaPort = getMappedPort(Constants.KAFKA_PORT);

        LOGGER.info("Mapped port: {}", kafkaDynamicKafkaPort);

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
                .append(" --override listeners=").append(kafkaListeners).append("PLAINTEXT://0.0.0.0:").append(Constants.KAFKA_PORT)
                .append(" --override advertised.listeners=").append(advertisedListeners)
                .append(" --override listener.security.protocol.map=").append(kafkaListenerSecurityProtocol).append("PLAINTEXT:PLAINTEXT")
                .append(" --override inter.broker.listener.name=BROKER1");

        if (this.kafkaConfigurationMap == null) {
            this.kafkaConfigurationMap = new HashMap<>();
        }

        this.kafkaConfigurationMap.put("broker.id", String.valueOf(this.brokerId));

        if (useKraft) {
            kafkaConfiguration
                    .append(" --override controller.listener.names=").append("BROKER1");
        } else {
            kafkaConfiguration
                    .append(" --override zookeeper.connect=localhost:").append(Constants.ZOOKEEPER_PORT);
        }

        // additional kafka config
        this.kafkaConfigurationMap.forEach((configName, configValue) ->
            kafkaConfiguration
                .append(" --override ")
                .append(configName)
                .append("=")
                .append(configValue));

        String command = "#!/bin/bash \n";

        if (!this.useKraft) {
            if (this.externalZookeeperConnect != null) {
                withEnv("KAFKA_ZOOKEEPER_CONNECT", this.externalZookeeperConnect);
            } else {
                command += "bin/zookeeper-server-start.sh config/zookeeper.properties &\n";
            }
            command += "bin/kafka-server-start.sh config/server.properties" + kafkaConfiguration;
        } else {
            command += "bin/kafka-storage.sh format -t " + this.storageUUID + " -c config/kraft/server.properties \n";
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
        return String.format("PLAINTEXT://%s:%s", getContainerIpAddress(), kafkaDynamicKafkaPort);
    }

    /**
     * Fluent method, which sets @code{kafkaConfigurationMap}.
     *
     * @param kafkaConfigurationMap kafka configuration
     * @return StrimziKafkaContainer instance
     */
    public StrimziKafkaContainer withKafkaConfigurationMap(final Map<String, String> kafkaConfigurationMap) {
        this.kafkaConfigurationMap = kafkaConfigurationMap;
        return this;
    }

    /**
     * Fluent method, which sets @code{externalZookeeperConnect}.
     *
     * If the broker was created using Kraft, this method throws an {@link IllegalArgumentException}.
     *
     * @param externalZookeeperConnect connect string
     * @return StrimziKafkaContainer instance
     */
    public StrimziKafkaContainer withExternalZookeeperConnect(final String externalZookeeperConnect) {
        if (useKraft) {
            throw new IllegalStateException("Cannot configure an external Zookeeper and use Kraft at the same time");
        }
        this.externalZookeeperConnect = externalZookeeperConnect;
        return self();
    }

    /**
     * Fluent method, which sets @code{brokerId}.
     *
     * @param brokerId broker id
     * @return StrimziKafkaContainer instance
     */
    public StrimziKafkaContainer withBrokerId(final int brokerId) {
        this.brokerId = brokerId;
        return self();
    }

    /**
     * Fluent method, which sets @code{kafkaVersion}.
     *
     * @param kafkaVersion kafka version
     * @return StrimziKafkaContainer instance
     */
    public StrimziKafkaContainer withKafkaVersion(final String kafkaVersion) {
        this.kafkaVersion = kafkaVersion;
        return self();
    }

    /**
     * Fluent method, which sets @code{withStrimziTestContainerImageVersion}.
     *
     * @param strimziTestContainerImageVersion strimzi test container image version
     * @return StrimziKafkaContainer instance
     */
    public StrimziKafkaContainer withStrimziTestContainerImageVersion(final String strimziTestContainerImageVersion) {
        this.strimziTestContainerImageVersion = strimziTestContainerImageVersion;
        return self();
    }

    /**
     * Fluent method, which sets @code{useKraft}.
     *
     * Flag to signal if we deploy Kafka with ZooKeeper or not.
     *
     * @param useKraft flag if we use Kraft
     * @return StrimziKafkaContainer instance
     */
    public StrimziKafkaContainer withKraft(final boolean useKraft) {
        this.useKraft = useKraft;
        return self();
    }

    /**
     * Fluent method, which sets @code{storageUUID}.
     * The storage UUID must be the 16 bytes of a base64-encoded UUID.
     *
     * This method requires the broker to be created using Kraft.
     *
     * @param uuid the uuid, must not be {@code null} or blank.
     * @return StrimziKafkaContainerBuilder instance
     */
    public StrimziKafkaContainer withStorageUUID(final String uuid) {
        if (!useKraft) {
            throw new IllegalStateException("Setting the storage UUID requires Kraft");
        }
        if (uuid == null || uuid.trim().isEmpty()) {
            throw new IllegalStateException("The UUID must not be blank");
        }
        this.storageUUID = uuid;
        return self();
    }
}
