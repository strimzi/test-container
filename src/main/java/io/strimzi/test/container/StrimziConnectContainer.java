/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

class StrimziConnectContainer extends GenericContainer<StrimziConnectContainer> {

    private static final String STARTER_SCRIPT = "/start_connect.sh";
    private static final String CONFIG_FILE = "/opt/kafka/config/connect.properties";

    private final StrimziKafkaCluster kafkaCluster;
    private final Properties configs;

    public StrimziConnectContainer(String imageName, StrimziKafkaCluster kafkaCluster, Properties configs) {
        super(imageName);
        this.kafkaCluster = kafkaCluster;
        this.configs = configs;
    }

    @Override
    @DoNotMutate
    protected void doStart() {
        super.setNetwork(kafkaCluster.getNetwork());
        super.setCommand("sh", "-c", "while [ ! -x " + STARTER_SCRIPT + " ]; do sleep 0.1; done; " + STARTER_SCRIPT);
        super.doStart();
    }

    @Override
    @DoNotMutate
    protected void containerIsStarting(final InspectContainerResponse containerInfo, final boolean reused) {
        super.containerIsStarting(containerInfo, reused);

        // Write configs to a file in the container
        StringWriter writer = new StringWriter();
        try {
            configs.store(writer, null);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build configuration file", e);
        }
        copyFileToContainer(
                Transferable.of(writer.toString().getBytes(StandardCharsets.UTF_8)),
                CONFIG_FILE);

        // Write starter script to a file in the container
        final String command = "/opt/kafka/bin/connect-distributed.sh " + CONFIG_FILE;
        copyFileToContainer(
                Transferable.of(command.getBytes(StandardCharsets.UTF_8), 700),
                STARTER_SCRIPT
        );
    }

    @DoNotMutate
    public StrimziConnectContainer waitForRunning() {
        super.waitingFor(Wait.forLogMessage(".*Finished starting connectors and tasks.*", 1));
        return this;
    }

    /* for testing */ Properties getConfigs() {
        return configs;
    }
}
