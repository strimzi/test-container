/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import com.github.dockerjava.api.command.InspectContainerResponse;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.strimzi.utils.LogicalKafkaVersionEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.Transferable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * StrimziZookeeperContainer is an instance of the Zookeeper encapsulated inside a docker container using image from
 * quay.io/strimzi/kafka with the given version. It can be combined with @StrimziKafkaContainer but we suggest to use
 * directly @StrimziKafkaCluster for more complicated testing.
 */
@SuppressFBWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
public class StrimziZookeeperContainer extends GenericContainer<StrimziZookeeperContainer> {

    private static final Logger LOGGER = LogManager.getLogger(StrimziZookeeperContainer.class);
    private static LogicalKafkaVersionEntity logicalKafkaVersionEntity;
    private static final String STARTER_SCRIPT = "/testcontainers_start.sh";

    static {
        try {
            logicalKafkaVersionEntity = new LogicalKafkaVersionEntity();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private StrimziZookeeperContainer(StrimziZookeeperContainerBuilder builder) {
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

        this.setDockerImageName("quay.io/strimzi-test-container/test-container:" +
            this.strimziTestContainerVersion + "-kafka-" +
            this.kafkaVersion);

        // we need this shared network in case we deploy StrimziKafkaCluster, which consist `StrimziZookeeperContainer`
        // instance and by default each container has its own network
        this.withNetwork(Network.SHARED);
        // exposing zookeeper port from the container
        this.withExposedPorts(ZOOKEEPER_PORT);
        this.withNetworkAliases("zookeeper");
        this.withEnv("LOG_DIR", "/tmp");
        this.withEnv("ZOOKEEPER_CLIENT_PORT", String.valueOf(ZOOKEEPER_PORT));
        // env for readiness
        this.withEnv("ZOO_4LW_COMMANDS_WHITELIST", "ruok");
    }

    /**
     * Default ZooKeeper port
     */
    public static final int ZOOKEEPER_PORT = 2181;
    private static int zookeeperDynamicExposedPort;

    private String kafkaVersion;
    private String strimziTestContainerVersion;

    @Override
    protected void doStart() {
        // we need it for the startZookeeper(); and startKafka(); to run container before...
        withCommand("sh", "-c", "while [ ! -f " + STARTER_SCRIPT + " ]; do sleep 0.1; done; " + STARTER_SCRIPT);
        super.doStart();
    }

    @Override
    protected void containerIsStarting(InspectContainerResponse containerInfo, boolean reused) {
        super.containerIsStarting(containerInfo, reused);

        zookeeperDynamicExposedPort = getMappedPort(ZOOKEEPER_PORT);

        LOGGER.info("This is mapped port {}", zookeeperDynamicExposedPort);

        final String command =
            "#!/bin/bash \n" +
                "bin/zookeeper-server-start.sh config/zookeeper.properties\n";

        LOGGER.info("Copying command to 'STARTER_SCRIPT' script.");

        copyFileToContainer(
            Transferable.of(command.getBytes(StandardCharsets.UTF_8), 700),
            STARTER_SCRIPT
        );
    }

    public static final class StrimziZookeeperContainerBuilder {
        private String kafkaVersion;
        private String strimziTestContainerVersion;
        public StrimziZookeeperContainerBuilder() {
        }
        public static StrimziZookeeperContainerBuilder aStrimziZookeeperContainer() {
            return new StrimziZookeeperContainerBuilder();
        }

        public StrimziZookeeperContainerBuilder withKafkaVersion(String kafkaVersion) {
            this.kafkaVersion = kafkaVersion;
            return this;
        }
        public StrimziZookeeperContainerBuilder withStrimziTestContainerVersion(String strimziTestContainerVersion) {
            this.strimziTestContainerVersion = strimziTestContainerVersion;
            return this;
        }
        public StrimziZookeeperContainer build() {
            return new StrimziZookeeperContainer(this);
        }
    }
}
