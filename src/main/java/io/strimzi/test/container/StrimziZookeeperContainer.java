/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import com.github.dockerjava.api.command.InspectContainerResponse;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.MountableFile;

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
    /**
     * Default ZooKeeper port
     */
    public static final int ZOOKEEPER_PORT = 2181;

    /**
     * Lazy image name provider
     */
    private final CompletableFuture<String> imageNameProvider;

    // instance attributes
    private String kafkaVersion;

    /**
     * Image name is specified lazily automatically in {@link #doStart()} method
     */
    public StrimziZookeeperContainer() {
        this(new CompletableFuture<>());
    }

    /**
     * Image name is specified by {@code dockerImageName}
     *
     * @param dockerImageName specific docker image name provided by constructor parameter
     */
    public StrimziZookeeperContainer(String dockerImageName) {
        this(CompletableFuture.completedFuture(dockerImageName));
    }

    /**
     * Image name is lazily set in {@link #doStart()} method
     */
    private StrimziZookeeperContainer(CompletableFuture<String> imageName) {
        super(imageName);
        this.imageNameProvider = imageName;
    }

    @Override
    protected void doStart() {
        if (!imageNameProvider.isDone()) {
            imageNameProvider.complete(KafkaVersionService.strimziTestContainerImageName(kafkaVersion));
        }
        // we need this shared network in case we deploy StrimziKafkaCluster, which consist `StrimziZookeeperContainer`
        // instance and by default each container has its own network
        super.setNetwork(Network.SHARED);
        // exposing zookeeper port from the container
        super.setExposedPorts(Collections.singletonList(ZOOKEEPER_PORT));
        super.setNetworkAliases(Collections.singletonList("zookeeper"));
        super.addEnv("LOG_DIR", "/tmp");
        super.addEnv("ZOOKEEPER_CLIENT_PORT", String.valueOf(ZOOKEEPER_PORT));
        // env for readiness
        super.addEnv("ZOO_4LW_COMMANDS_WHITELIST", "ruok");
        // we need it for the startZookeeper(); and startKafka(); to run container before...
        withCommand("sh", "-c", "while [ ! -f " + STARTER_SCRIPT + " ]; do sleep 0.1; done; " + STARTER_SCRIPT);
        super.doStart();
    }

    @Override
    protected void containerIsStarting(InspectContainerResponse containerInfo, boolean reused) {
        super.containerIsStarting(containerInfo, reused);

        int zookeeperDynamicExposedPort = getMappedPort(ZOOKEEPER_PORT);

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
     * Fluent method, copy server properties file to the container
     *
     * @param zooKeeperPropertiesFile the mountable config file
     * @return StrimziZookeeperContainer instance
     */
    public StrimziZookeeperContainer withZooKeeperPropertiesFile(final MountableFile zooKeeperPropertiesFile) {
        withCopyFileToContainer(zooKeeperPropertiesFile, "/opt/kafka/config/zookeeper.properties");
        return this;
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

    /**
     * Provides host and mapped port, by which it connects to the ZooKeeper instance
     *
     * @return zookeeper connect string `host:port`
     */
    public String getConnectString() {
        return this.getHost() + ":" + this.getFirstMappedPort();
    }
}
