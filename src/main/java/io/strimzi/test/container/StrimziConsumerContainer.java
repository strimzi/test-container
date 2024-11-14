/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.groupcdg.pitest.annotations.DoNotMutate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.MountableFile;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * StrimziConsumerContainer is a specialized container that wraps the Kafka console consumer.
 * It allows you to configure and run the consumer with various options in integration tests.
 */
public class StrimziConsumerContainer extends GenericContainer<StrimziConsumerContainer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(StrimziConsumerContainer.class);

    /**
     * Lazy image name provider
     */
    private final CompletableFuture<String> imageNameProvider;
    private final List<String> commandOptions = new ArrayList<>();

    private String kafkaVersion;

    /**
     * The file containing the startup script.
     */
    public static final String STARTER_SCRIPT = "/testcontainers_start.sh";

    public StrimziConsumerContainer() {
        this(new CompletableFuture<>());
    }

    public StrimziConsumerContainer(String dockerImageName) {
        this(CompletableFuture.completedFuture(dockerImageName));
    }

    /**
     * Image name is lazily set in {@link #doStart()} method
     */
    private StrimziConsumerContainer(CompletableFuture<String> imageName) {
        super(imageName);

        this.imageNameProvider = imageName;
        super.setNetwork(Network.SHARED);
    }

    // Builder methods for options

    /**
     * Sets the bootstrap server.
     *
     * @param bootstrapServer the bootstrap server address
     * @return the StrimziConsumerContainer instance
     */
    public StrimziConsumerContainer withBootstrapServer(String bootstrapServer) {
        this.commandOptions.add("--bootstrap-server " + bootstrapServer);
        return self();
    }

    /**
     * Adds a consumer property in key=value format.
     *
     * @param key   the property key
     * @param value the property value
     * @return the StrimziConsumerContainer instance
     */
    public StrimziConsumerContainer withConsumerProperty(String key, String value) {
        this.commandOptions.add("--consumer-property " + key + "=" + value);
        return self();
    }

    /**
     * Sets the consumer configuration file.
     *
     * @param configFile the configuration file to copy into the container
     * @return the StrimziConsumerContainer instance
     */
    public StrimziConsumerContainer withConsumerConfig(MountableFile configFile) {
        this.withCopyFileToContainer(configFile, "/tmp/consumer.properties");
        this.commandOptions.add("--consumer.config /tmp/consumer.properties");
        return self();
    }

    /**
     * Enables events logging.
     *
     * @return the StrimziConsumerContainer instance
     */
    public StrimziConsumerContainer withEnableSystestEvents() {
        this.commandOptions.add("--enable-systest-events");
        return self();
    }

    /**
     * Sets the formatter class.
     *
     * @param formatterClass the formatter class name
     * @return the StrimziConsumerContainer instance
     */
    public StrimziConsumerContainer withFormatter(String formatterClass) {
        this.commandOptions.add("--formatter " + formatterClass);
        return self();
    }

    /**
     * Sets the formatter configuration file.
     *
     * @param configFile the configuration file to copy into the container
     * @return the StrimziConsumerContainer instance
     */
    public StrimziConsumerContainer withFormatterConfig(MountableFile configFile) {
        this.withCopyFileToContainer(configFile, "/tmp/formatter.properties");
        this.commandOptions.add("--formatter-config /tmp/formatter.properties");
        return self();
    }

    /**
     * Starts consuming from the beginning.
     *
     * @return the StrimziConsumerContainer instance
     */
    public StrimziConsumerContainer withFromBeginning() {
        this.commandOptions.add("--from-beginning");
        return self();
    }

    /**
     * Sets the consumer group id.
     *
     * @param groupId the consumer group id
     * @return the StrimziConsumerContainer instance
     */
    public StrimziConsumerContainer withGroup(String groupId) {
        this.commandOptions.add("--group " + groupId);
        return self();
    }

    /**
     * Includes topics matching the given regular expression.
     *
     * @param includePattern the regular expression pattern
     * @return the StrimziConsumerContainer instance
     */
    public StrimziConsumerContainer withInclude(String includePattern) {
        this.commandOptions.add("--include " + includePattern);
        return self();
    }

    /**
     * Sets the isolation level.
     *
     * @param isolationLevel the isolation level ('read_committed' or 'read_uncommitted')
     * @return the StrimziConsumerContainer instance
     */
    public StrimziConsumerContainer withIsolationLevel(String isolationLevel) {
        this.commandOptions.add("--isolation-level " + isolationLevel);
        return self();
    }

    /**
     * Sets the key deserializer class.
     *
     * @param keyDeserializer the key deserializer class name
     * @return the StrimziConsumerContainer instance
     */
    public StrimziConsumerContainer withKeyDeserializer(String keyDeserializer) {
        this.commandOptions.add("--key-deserializer " + keyDeserializer);
        return self();
    }

    /**
     * Sets the maximum number of messages to consume before exiting.
     *
     * @param maxMessages the maximum number of messages
     * @return the StrimziConsumerContainer instance
     */
    public StrimziConsumerContainer withMaxMessages(int maxMessages) {
        this.commandOptions.add("--max-messages " + maxMessages);
        return self();
    }

    /**
     * Sets the offset to consume from.
     *
     * @param offset the offset to consume from ('earliest', 'latest', or a non-negative number)
     * @return the StrimziConsumerContainer instance
     */
    public StrimziConsumerContainer withOffset(String offset) {
        this.commandOptions.add("--offset " + offset);
        return self();
    }

    /**
     * Sets the partition to consume from.
     *
     * @param partition the partition number
     * @return the StrimziConsumerContainer instance
     */
    public StrimziConsumerContainer withPartition(int partition) {
        this.commandOptions.add("--partition " + partition);
        return self();
    }

    /**
     * Adds a formatter property in key=value format.
     *
     * @param key   the property key
     * @param value the property value
     * @return the StrimziConsumerContainer instance
     */
    public StrimziConsumerContainer withProperty(String key, String value) {
        this.commandOptions.add("--property " + key + "=" + value);
        return self();
    }

    /**
     * Skips messages on error.
     *
     * @return the StrimziConsumerContainer instance
     */
    public StrimziConsumerContainer withSkipMessageOnError() {
        this.commandOptions.add("--skip-message-on-error");
        return self();
    }

    /**
     * Sets the timeout in milliseconds.
     *
     * @param timeoutMs the timeout
     * @return the StrimziConsumerContainer instance
     */
    public StrimziConsumerContainer withTimeoutMs(long timeoutMs) {
        this.commandOptions.add("--timeout-ms " + timeoutMs);
        return self();
    }

    /**
     * Sets the topic to consume messages from.
     *
     * @param topic the topic name
     * @return the StrimziConsumerContainer instance
     */
    public StrimziConsumerContainer withTopic(String topic) {
        this.commandOptions.add("--topic " + topic);
        return self();
    }

    /**
     * Sets the value deserializer class.
     *
     * @param valueDeserializer the value deserializer class name
     * @return the StrimziConsumerContainer instance
     */
    public StrimziConsumerContainer withValueDeserializer(String valueDeserializer) {
        this.commandOptions.add("--value-deserializer " + valueDeserializer);
        return self();
    }

    /**
     * Displays the Kafka version.
     *
     * @return the StrimziConsumerContainer instance
     */
    public StrimziConsumerContainer withVersion() {
        this.commandOptions.add("--version");
        return self();
    }

    /**
     * Includes topics matching the given regular expression (deprecated).
     *
     * @param whitelistPattern the regular expression pattern
     * @return the StrimziConsumerContainer instance
     */
    public StrimziConsumerContainer withWhitelist(String whitelistPattern) {
        this.commandOptions.add("--whitelist " + whitelistPattern);
        return self();
    }

    /**
     * Enables logging output from the container to the SLF4J logger.
     *
     * @return the StrimziConsumerContainer instance
     */
    public StrimziConsumerContainer withLogging() {
        // Attach a log consumer to the container
        this.withLogConsumer(new Slf4jLogConsumer(LOGGER).withPrefix(this.getClass().getName()));
        return self();
    }

    /**
     * Allows overriding the startup script command.
     * The default is: {@code "while [ ! -x " + STARTER_SCRIPT + " ]; do sleep 0.1; done; " + STARTER_SCRIPT}
     *
     * @return the command
     */
    @DoNotMutate
    protected String runStarterScript() {
        return "while [ ! -x " + STARTER_SCRIPT + " ]; do sleep 0.1; done; " + STARTER_SCRIPT;
    }

    @Override
    @DoNotMutate
    protected void doStart() {
        if (!this.imageNameProvider.isDone()) {
            this.imageNameProvider.complete(KafkaVersionService.strimziTestContainerImageName(this.kafkaVersion));
        }

        super.setCommand("sh", "-c", runStarterScript());
        super.doStart();
    }

    @Override
    @DoNotMutate
    protected void containerIsStarting(final InspectContainerResponse containerInfo, final boolean reused) {
        super.containerIsStarting(containerInfo, reused);

        // Build the command to run kafka-console-consumer.sh with options
        String command = "#!/bin/bash\n";
        command += "bin/kafka-console-consumer.sh " + String.join(" ", this.commandOptions) + "\n";

        LOGGER.info("Copying command:\n{} to 'STARTER_SCRIPT' script.", command);

        // Copy the command script into the container
        copyFileToContainer(
            Transferable.of(command.getBytes(StandardCharsets.UTF_8), 700),
            STARTER_SCRIPT
        );
    }

    /**
     * Sets the Kafka version to use.
     *
     * @param kafkaVersion the Kafka version
     * @return the StrimziConsumerContainer instance
     */
    public StrimziConsumerContainer withKafkaVersion(String kafkaVersion) {
        this.kafkaVersion = kafkaVersion;
        return self();
    }

    public List<String> getCommandOptions() {
        return commandOptions;
    }

    public String getKafkaVersion() {
        return kafkaVersion;
    }
}