/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import com.github.dockerjava.api.command.InspectContainerResponse;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.strimzi.test.container.utils.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.Transferable;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * StrimziZookeeperContainer is an instance of the Zookeeper encapsulated inside a docker container using image from
 * quay.io/strimzi/kafka with the given version. It can be combined with @StrimziKafkaContainer but we suggest to use
 * directly @StrimziKafkaCluster for more complicated testing.
 */
@SuppressFBWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
public class StrimziZookeeperContainer extends GenericContainer<StrimziZookeeperContainer> {

    // class attributes
    private static final Logger LOGGER = LoggerFactory.getLogger(StrimziZookeeperContainer.class);
    private static final String STARTER_SCRIPT = "/testcontainers_start.sh";

    // instance attributes
    private String kafkaVersion;

    /**
     * Image name is lazily set in {@link #doStart()} method
     */
    public StrimziZookeeperContainer() {
        super(CompletableFuture.completedFuture(null));
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
        this.setDockerImageName(KafkaVersionService.strimziTestContainerImageName(kafkaVersion));
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

    /**
     * Fluent method, which sets @code{kafkaVersion}.
     *
     * @param kafkaVersion kafka version
     * @return StrimziKafkaContainer instance
     */
    public StrimziZookeeperContainer withKafkaVersion(final String kafkaVersion) {
        this.kafkaVersion = kafkaVersion;
        return this;
    }

}
