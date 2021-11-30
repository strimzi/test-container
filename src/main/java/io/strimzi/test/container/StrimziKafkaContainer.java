/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ContainerNetwork;
import io.strimzi.utils.LogicalKafkaVersionEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.images.builder.Transferable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
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
    private static LogicalKafkaVersionEntity logicalKafkaVersionEntity;

    private StrimziKafkaContainer(StrimziKafkaContainerBuilder builder) throws IOException {
        logicalKafkaVersionEntity = new LogicalKafkaVersionEntity();

        if (builder.strimziTestContainerVersion == null || builder.strimziTestContainerVersion.isEmpty()) {
            this.strimziTestContainerVersion = logicalKafkaVersionEntity.latestRelease().getStrimziTestContainerVersion();
            LOGGER.info("You did not specify Strimzi test container version. Using latest release:{}", this.strimziTestContainerVersion);
        } else {
            this.strimziTestContainerVersion = builder.strimziTestContainerVersion;
        }
        if (builder.kafkaVersion == null || builder.kafkaVersion.isEmpty()) {
            this.kafkaVersion = logicalKafkaVersionEntity.latestRelease().getVersion();
            LOGGER.info("You did not specify Kafka version. Using latest release:{}", this.kafkaVersion);
        } else {
            this.kafkaVersion = builder.kafkaVersion;
        }

        this.useKraft = builder.useKraft;
        this.storageUUID = builder.storageUUID == null ? DEFAUT_STORAGE_UUID : builder.storageUUID;
        this.brokerId = builder.brokerId;

        if (builder.kafkaConfigurationMap == null) {
            this.kafkaConfigurationMap = new HashMap<>();
        } else {
            this.kafkaConfigurationMap = new HashMap<>();
            this.kafkaConfigurationMap.putAll(builder.kafkaConfigurationMap);
        }

        LOGGER.info("Set broker.id to: {}", this.brokerId);
        this.kafkaConfigurationMap.put("broker.id", String.valueOf(this.brokerId));

        LOGGER.info("Injected configuration inside Kafka Container constructor....\n{}", this.kafkaConfigurationMap.toString());

        this.externalZookeeperConnect = builder.externalZookeeperConnect;

        // we need this shared network in case we deploy StrimziKafkaCluster which consist of `StrimziKafkaContainer`
        // instances and by default each container has its own network, which results in `Unable to resolve address: zookeeper:2181`
        this.withNetwork(Network.SHARED);
        // exposing kafka port from the container
        this.withExposedPorts(KAFKA_PORT);
        this.withEnv("LOG_DIR", "/tmp");

        this.setDockerImageName("quay.io/strimzi-test-container/test-container:" +
            this.strimziTestContainerVersion + "-kafka-" +
            this.kafkaVersion);

        LOGGER.info("=============================================");
        LOGGER.info(this.getDockerImageName());
        LOGGER.info("=============================================");

        LOGGER.info("This is inside this object:{}", this::toString);
    }

    /**
     * Default Kafka port
     */
    public static final int KAFKA_PORT = 9092;
    public static int kafkaDynamicKafkaPort;
    private static final String DEFAUT_STORAGE_UUID = "xtzWWN4bTjitpL4efd9s6g";

    /**
     * Default ZooKeeper port
     */
    public static final int ZOOKEEPER_PORT = 2181;

    private static final String STARTER_SCRIPT = "/testcontainers_start.sh";

    private Map<String, String> kafkaConfigurationMap;
    private String externalZookeeperConnect;
    private int brokerId;
    private String kafkaVersion;
    private String strimziTestContainerVersion;
    private boolean useKraft;
    private String storageUUID;

    @Override
    protected void doStart() {
        // we need it for the startZookeeper(); and startKafka(); to run container before...
        withCommand("sh", "-c", "while [ ! -f " + STARTER_SCRIPT + " ]; do sleep 0.1; done; " + STARTER_SCRIPT);
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

        kafkaDynamicKafkaPort = getMappedPort(KAFKA_PORT);

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
                .append(" --override listeners=").append(kafkaListeners).append("PLAINTEXT://0.0.0.0:").append(KAFKA_PORT)
                .append(" --override advertised.listeners=").append(advertisedListeners)
                .append(" --override listener.security.protocol.map=").append(kafkaListenerSecurityProtocol).append("PLAINTEXT:PLAINTEXT")
                .append(" --override inter.broker.listener.name=BROKER1");

        LOGGER.info("Injected configuration inside Kafka Container....\n{}", this.kafkaConfigurationMap.toString());

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
        return String.format("PLAINTEXT://%s:%s", getContainerIpAddress(), kafkaDynamicKafkaPort);
    }

    public final static class StrimziKafkaContainerBuilder {
        private Map<String, String> kafkaConfigurationMap;
        private String externalZookeeperConnect;
        private int brokerId;
        private String kafkaVersion;
        private String strimziTestContainerVersion;
        private boolean useKraft;
        private String storageUUID;

        public StrimziKafkaContainerBuilder withKafkaConfigurationMap(Map<String, String> kafkaConfigurationMap) {
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
        public StrimziKafkaContainerBuilder withExternalZookeeperConnect(String externalZookeeperConnect) {
            if (useKraft) {
                throw new IllegalArgumentException("Cannot configure an external Zookeeper and use Kraft at the same time");
            }
            this.externalZookeeperConnect = externalZookeeperConnect;
            return this;
        }
        public StrimziKafkaContainerBuilder withBrokerId(int brokerId) {
            this.brokerId = brokerId;
            return this;
        }
        public StrimziKafkaContainerBuilder withKafkaVersion(String kafkaVersion) {
            this.kafkaVersion = kafkaVersion;
            return this;
        }
        public StrimziKafkaContainerBuilder withStrimziTestContainerVersion(String strimziTestContainerVersion) {
            this.strimziTestContainerVersion = strimziTestContainerVersion;
            return this;
        }
        public StrimziKafkaContainerBuilder withKraft(boolean useKraft) {
            this.useKraft = useKraft;
            return this;
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
        public StrimziKafkaContainerBuilder withStorageUUID(final String uuid) {
            if (!useKraft) {
                throw new IllegalArgumentException("Setting the storage UUID requires Kraft");
            }
            if (uuid == null || uuid.trim().isEmpty()) {
                throw new IllegalArgumentException("The UUID must not be blank");
            }
            this.storageUUID = uuid;
            return this;
        }

        public StrimziKafkaContainer build() throws IOException {
            return new StrimziKafkaContainer(this);
        }
    }
}
