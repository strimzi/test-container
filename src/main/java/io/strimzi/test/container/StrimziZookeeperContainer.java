/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import com.github.dockerjava.api.command.InspectContainerResponse;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.strimzi.utils.Constants;
import io.strimzi.utils.LogicalKafkaVersionEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.Transferable;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

/**
 * StrimziZookeeperContainer is an instance of the Zookeeper encapsulated inside a docker container using image from
 * quay.io/strimzi/kafka with the given version. It can be combined with @StrimziKafkaContainer but we suggest to use
 * directly @StrimziKafkaCluster for more complicated testing.
 */
@SuppressFBWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
public class StrimziZookeeperContainer extends GenericContainer<StrimziZookeeperContainer> {

    // class attributes
    private static final Logger LOGGER = LogManager.getLogger(StrimziZookeeperContainer.class);
    private static final LogicalKafkaVersionEntity LOGICAL_KAFKA_VERSION_ENTITY;
    private static final String STARTER_SCRIPT = "/testcontainers_start.sh";

    static {
        LOGICAL_KAFKA_VERSION_ENTITY = new LogicalKafkaVersionEntity();
    }

    private StrimziZookeeperContainer(StrimziZookeeperContainerBuilder builder) {
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
        this.setDockerImageName("quay.io/strimzi-test-container/test-container:" +
            strimziTestContainerVersion + "-kafka-" +
            kafkaVersion);
        // we need this shared network in case we deploy StrimziKafkaCluster, which consist `StrimziZookeeperContainer`
        // instance and by default each container has its own network
        super.setNetwork(Network.SHARED);
        // exposing zookeeper port from the container
        super.setExposedPorts(Collections.singletonList(Constants.ZOOKEEPER_PORT));
        super.setNetworkAliases(Collections.singletonList("zookeeper"));
        super.addEnv("LOG_DIR", "/tmp");
        super.addEnv("ZOOKEEPER_CLIENT_PORT", String.valueOf(Constants.ZOOKEEPER_PORT));
        // env for readiness
        super.addEnv("ZOO_4LW_COMMANDS_WHITELIST", "ruok");
    }

    @Override
    protected void doStart() {
        // we need it for the startZookeeper(); and startKafka(); to run container before...
        withCommand("sh", "-c", "while [ ! -f " + STARTER_SCRIPT + " ]; do sleep 0.1; done; " + STARTER_SCRIPT);
        super.doStart();
    }

    @Override
    protected void containerIsStarting(InspectContainerResponse containerInfo, boolean reused) {
        super.containerIsStarting(containerInfo, reused);

        int zookeeperDynamicExposedPort = getMappedPort(Constants.ZOOKEEPER_PORT);

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

    public static final class StrimziZookeeperContainerBuilder extends GenericContainer<StrimziZookeeperContainerBuilder> {
        private String kafkaVersion;
        private String strimziTestContainerVersion;
        public StrimziZookeeperContainerBuilder() {
        }

        public StrimziZookeeperContainerBuilder withKafkaVersion(final String kafkaVersion) {
            this.kafkaVersion = kafkaVersion;
            return this;
        }
        public StrimziZookeeperContainerBuilder withStrimziTestContainerVersion(final String strimziTestContainerVersion) {
            this.strimziTestContainerVersion = strimziTestContainerVersion;
            return this;
        }
        public StrimziZookeeperContainer build() {
            return new StrimziZookeeperContainer(this);
        }
    }
}
