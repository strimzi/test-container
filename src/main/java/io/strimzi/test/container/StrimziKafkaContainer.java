/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ContainerNetwork;
import io.strimzi.utils.Constants;
import io.strimzi.utils.LogicalKafkaVersionEntity;
import io.strimzi.utils.Utils;
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
    private static final LogicalKafkaVersionEntity LOGICAL_KAFKA_VERSION_ENTITY;
    private static int kafkaDynamicKafkaPort;

    // instance attributes
    private final Map<String, String> kafkaConfigurationMap;
    private final String externalZookeeperConnect;
    private final boolean useKraft;
    private final String storageUUID;

    static {
        LOGICAL_KAFKA_VERSION_ENTITY = new LogicalKafkaVersionEntity();
    }

    private StrimziKafkaContainer(StrimziKafkaContainerBuilder builder) {
        String strimziTestContainerVersion;
        if (builder.strimziTestContainerVersion == null || builder.strimziTestContainerVersion.isEmpty()) {
            strimziTestContainerVersion = LOGICAL_KAFKA_VERSION_ENTITY.latestRelease().getStrimziTestContainerVersion();
            LOGGER.info("You did not specify Strimzi test container version. Using latest release:{}", strimziTestContainerVersion);
        } else {
            strimziTestContainerVersion = builder.strimziTestContainerVersion;
        }
        String kafkaVersion;
        if (builder.kafkaVersion == null || builder.kafkaVersion.isEmpty()) {
            kafkaVersion = LOGICAL_KAFKA_VERSION_ENTITY.latestRelease().getVersion();
            LOGGER.info("You did not specify Kafka version. Using latest release:{}", kafkaVersion);
        } else {
            kafkaVersion = builder.kafkaVersion;
        }
        this.useKraft = builder.useKraft;
        this.storageUUID = builder.storageUUID == null ? Utils.randomUuid().toString() : builder.storageUUID;
        int brokerId = builder.brokerId;

        if (builder.kafkaConfigurationMap == null) {
            this.kafkaConfigurationMap = new HashMap<>();
        } else {
            this.kafkaConfigurationMap = new HashMap<>();
            this.kafkaConfigurationMap.putAll(builder.kafkaConfigurationMap);
        }

        LOGGER.info("Set broker.id to: {}", brokerId);
        this.kafkaConfigurationMap.put("broker.id", String.valueOf(brokerId));

        LOGGER.info("Injected configuration inside Kafka Container constructor:\n{}", this.kafkaConfigurationMap.toString());

        this.externalZookeeperConnect = builder.externalZookeeperConnect;

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

        LOGGER.info("Injected configuration inside Kafka Container....\n{}", this.kafkaConfigurationMap.toString());

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

    public final static class StrimziKafkaContainerBuilder extends GenericContainer<StrimziKafkaContainerBuilder> {
        private Map<String, String> kafkaConfigurationMap;
        private String externalZookeeperConnect;
        private int brokerId;
        private String kafkaVersion;
        private String strimziTestContainerVersion;
        private boolean useKraft;
        private String storageUUID;

        public StrimziKafkaContainerBuilder withKafkaConfigurationMap(final Map<String, String> kafkaConfigurationMap) {
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
        public StrimziKafkaContainerBuilder withExternalZookeeperConnect(final String externalZookeeperConnect) {
            if (useKraft) {
                throw new IllegalArgumentException("Cannot configure an external Zookeeper and use Kraft at the same time");
            }
            this.externalZookeeperConnect = externalZookeeperConnect;
            return this;
        }
        public StrimziKafkaContainerBuilder withBrokerId(final int brokerId) {
            this.brokerId = brokerId;
            return this;
        }
        public StrimziKafkaContainerBuilder withKafkaVersion(final String kafkaVersion) {
            this.kafkaVersion = kafkaVersion;
            return this;
        }
        public StrimziKafkaContainerBuilder withStrimziTestContainerVersion(final String strimziTestContainerVersion) {
            this.strimziTestContainerVersion = strimziTestContainerVersion;
            return this;
        }
        public StrimziKafkaContainerBuilder withKraft(final boolean useKraft) {
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
