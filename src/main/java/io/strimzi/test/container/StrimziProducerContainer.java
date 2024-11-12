/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import com.github.dockerjava.api.command.InspectContainerResponse;
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

public class StrimziProducerContainer extends GenericContainer<StrimziProducerContainer> {

    // class attributes
    private static final Logger LOGGER = LoggerFactory.getLogger(StrimziProducerContainer.class);

    /**
     * Lazy image name provider
     */
    private final CompletableFuture<String> imageNameProvider;
    private final List<String> commandOptions = new ArrayList<>();

    private String kafkaVersion;
    private String messageInput;

    /**
     * The file containing the startup script.
     */
    public static final String STARTER_SCRIPT = "/testcontainers_start.sh";

    public StrimziProducerContainer() {
        this(new CompletableFuture<>());
    }

    public StrimziProducerContainer(String dockerImageName) {
        this(CompletableFuture.completedFuture(dockerImageName));
    }

    /**
     * Image name is lazily set in {@link #doStart()} method
     */
    private StrimziProducerContainer(CompletableFuture<String> imageName) {
        super(imageName);

        this.imageNameProvider = imageName;
        super.setNetwork(Network.SHARED);
    }

    // Builder methods for options

    /**
     * Sets the batch size.
     *
     * @param batchSize number of messages in a batch
     * @return the StrimziProducerContainer instance
     */
    public StrimziProducerContainer withBatchSize(int batchSize) {
        this.commandOptions.add("--batch-size " + batchSize);
        return self();
    }

    /**
     * Sets the bootstrap server.
     *
     * @param bootstrapServer the bootstrap server address
     * @return the StrimziProducerContainer instance
     */
    public StrimziProducerContainer withBootstrapServer(String bootstrapServer) {
        this.commandOptions.add("--bootstrap-server " + bootstrapServer);
        return self();
    }

    /**
     * Sets the broker list (deprecated in favor of bootstrap-server).
     *
     * @param brokerList the broker list
     * @return the StrimziProducerContainer instance
     */
    public StrimziProducerContainer withBrokerList(String brokerList) {
        this.commandOptions.add("--broker-list " + brokerList);
        return self();
    }

    /**
     * Sets the compression codec without specifying a value (defaults to 'gzip').
     *
     * @return the StrimziProducerContainer instance
     */
    public StrimziProducerContainer withCompressionCodec() {
        this.commandOptions.add("--compression-codec");
        return self();
    }

    /**
     * Sets the compression codec with a specified value.
     *
     * @param codec the compression codec ('none', 'gzip', 'snappy', 'lz4', 'zstd')
     * @return the StrimziProducerContainer instance
     */
    public StrimziProducerContainer withCompressionCodec(String codec) {
        this.commandOptions.add("--compression-codec " + codec);
        return self();
    }

    /**
     * Sets the line reader class.
     *
     * @param readerClass the line reader class name
     * @return the StrimziProducerContainer instance
     */
    public StrimziProducerContainer withLineReader(String readerClass) {
        this.commandOptions.add("--line-reader " + readerClass);
        return self();
    }

    /**
     * Sets the maximum block time in milliseconds.
     *
     * @param maxBlockMs the maximum block time
     * @return the StrimziProducerContainer instance
     */
    public StrimziProducerContainer withMaxBlockMs(long maxBlockMs) {
        this.commandOptions.add("--max-block-ms " + maxBlockMs);
        return self();
    }

    /**
     * Sets the maximum memory bytes.
     *
     * @param maxMemoryBytes the maximum memory in bytes
     * @return the StrimziProducerContainer instance
     */
    public StrimziProducerContainer withMaxMemoryBytes(long maxMemoryBytes) {
        this.commandOptions.add("--max-memory-bytes " + maxMemoryBytes);
        return self();
    }

    /**
     * Sets the maximum partition memory bytes.
     *
     * @param maxPartitionMemoryBytes the maximum partition memory in bytes
     * @return the StrimziProducerContainer instance
     */
    public StrimziProducerContainer withMaxPartitionMemoryBytes(int maxPartitionMemoryBytes) {
        this.commandOptions.add("--max-partition-memory-bytes " + maxPartitionMemoryBytes);
        return self();
    }

    /**
     * Sets the maximum number of message send retries.
     *
     * @param messageSendMaxRetries the maximum number of retries
     * @return the StrimziProducerContainer instance
     */
    public StrimziProducerContainer withMessageSendMaxRetries(int messageSendMaxRetries) {
        this.commandOptions.add("--message-send-max-retries " + messageSendMaxRetries);
        return self();
    }

    /**
     * Sets the metadata expiry time in milliseconds.
     *
     * @param metadataExpiryMs the metadata expiry time
     * @return the StrimziProducerContainer instance
     */
    public StrimziProducerContainer withMetadataExpiryMs(long metadataExpiryMs) {
        this.commandOptions.add("--metadata-expiry-ms " + metadataExpiryMs);
        return self();
    }

    /**
     * Adds a producer property in key=value format.
     *
     * @param key   the property key
     * @param value the property value
     * @return the StrimziProducerContainer instance
     */
    public StrimziProducerContainer withProducerProperty(String key, String value) {
        this.commandOptions.add("--producer-property " + key + "=" + value);
        return self();
    }

    /**
     * Sets the producer configuration file.
     *
     * @param configFile the configuration file to copy into the container
     * @return the StrimziProducerContainer instance
     */
    public StrimziProducerContainer withProducerConfig(MountableFile configFile) {
        this.withCopyFileToContainer(configFile, "/tmp/producer.properties");
        this.commandOptions.add("--producer.config /tmp/producer.properties");
        return self();
    }

    /**
     * Adds a message reader property in key=value format.
     *
     * @param key   the property key
     * @param value the property value
     * @return the StrimziProducerContainer instance
     */
    public StrimziProducerContainer withProperty(String key, String value) {
        this.commandOptions.add("--property " + key + "=" + value);
        return self();
    }

    /**
     * Sets the reader configuration file.
     *
     * @param configFile the configuration file to copy into the container
     * @return the StrimziProducerContainer instance
     */
    public StrimziProducerContainer withReaderConfig(MountableFile configFile) {
        this.withCopyFileToContainer(configFile, "/tmp/reader.properties");
        this.commandOptions.add("--reader-config /tmp/reader.properties");
        return self();
    }

    /**
     * Sets the required acks for producer requests.
     *
     * @param requestRequiredAcks the required acks
     * @return the StrimziProducerContainer instance
     */
    public StrimziProducerContainer withRequestRequiredAcks(String requestRequiredAcks) {
        this.commandOptions.add("--request-required-acks " + requestRequiredAcks);
        return self();
    }

    /**
     * Sets the request timeout in milliseconds.
     *
     * @param requestTimeoutMs the request timeout
     * @return the StrimziProducerContainer instance
     */
    public StrimziProducerContainer withRequestTimeoutMs(int requestTimeoutMs) {
        this.commandOptions.add("--request-timeout-ms " + requestTimeoutMs);
        return self();
    }

    /**
     * Sets the retry backoff time in milliseconds.
     *
     * @param retryBackoffMs the retry backoff time
     * @return the StrimziProducerContainer instance
     */
    public StrimziProducerContainer withRetryBackoffMs(long retryBackoffMs) {
        this.commandOptions.add("--retry-backoff-ms " + retryBackoffMs);
        return self();
    }

    /**
     * Sets the socket buffer size.
     *
     * @param socketBufferSize the socket buffer size
     * @return the StrimziProducerContainer instance
     */
    public StrimziProducerContainer withSocketBufferSize(int socketBufferSize) {
        this.commandOptions.add("--socket-buffer-size " + socketBufferSize);
        return self();
    }

    /**
     * Enables synchronous message sending.
     *
     * @return the StrimziProducerContainer instance
     */
    public StrimziProducerContainer withSync() {
        this.commandOptions.add("--sync");
        return self();
    }

    /**
     * Sets the timeout in milliseconds.
     *
     * @param timeoutMs the timeout
     * @return the StrimziProducerContainer instance
     */
    public StrimziProducerContainer withTimeout(long timeoutMs) {
        this.commandOptions.add("--timeout " + timeoutMs);
        return self();
    }

    /**
     * Sets the topic to produce messages to.
     *
     * @param topic the topic name
     * @return the StrimziProducerContainer instance
     */
    public StrimziProducerContainer withTopic(String topic) {
        this.commandOptions.add("--topic " + topic);
        return self();
    }

    /**
     * Displays the Kafka version.
     *
     * @return the StrimziProducerContainer instance
     */
    public StrimziProducerContainer withVersion() {
        this.commandOptions.add("--version");
        return self();
    }

    /**
     * Specifies message content to produce, directly as a string.
     * Each line in the string will be treated as a separate message.
     *
     * @param messageContent the message content
     * @return the StrimziProducerContainer instance
     */
    public StrimziProducerContainer withMessageContent(String messageContent) {
        this.messageInput = "echo \"" + messageContent.replace("\"", "\\\"") + "\"";
        return self();
    }

    /**
     * Specifies a file containing messages to produce.
     * Each line in the file will be treated as a separate message.
     *
     * @param messageFile the message file to be copied to the container
     * @return the StrimziProducerContainer instance
     */
    public StrimziProducerContainer withMessageFile(MountableFile messageFile) {
        this.withCopyFileToContainer(messageFile, "/tmp/messages.txt");
        this.messageInput = "cat /tmp/messages.txt";
        return self();
    }

    /**
     * Enables logging output from the container to the SLF4J logger.
     *
     * @return the StrimziProducerContainer instance
     */
    public StrimziProducerContainer withLogging() {
        // Attach a log consumer to the container
        this.withLogConsumer(new Slf4jLogConsumer(LOGGER).withPrefix(this.getClass().getName()));
        return self();
    }

    /**
     * Allows overriding the startup script command.
     * The default is: <pre>{@code "while [ ! -x " + STARTER_SCRIPT + " ]; do sleep 0.1; done; " + STARTER_SCRIPT}</pre>
     *
     * @return the command
     */
    protected String runStarterScript() {
        return "while [ ! -x " + STARTER_SCRIPT + " ]; do sleep 0.1; done; " + STARTER_SCRIPT;
    }

    @Override
    protected void doStart() {
        if (!this.imageNameProvider.isDone()) {
            this.imageNameProvider.complete(KafkaVersionService.strimziTestContainerImageName(this.kafkaVersion));
        }

        super.setCommand("sh", "-c", runStarterScript());
        super.doStart();
    }

    @Override
    protected void containerIsStarting(final InspectContainerResponse containerInfo, final boolean reused) {
        super.containerIsStarting(containerInfo, reused);

        // Build the command to run kafka-console-producer.sh with options
        String command = "#!/bin/bash\n";
        command += this.messageInput + " | bin/kafka-console-producer.sh " + String.join(" ", this.commandOptions) + "\n";

        LOGGER.info("Copying command:\n{} to 'STARTER_SCRIPT' script.", command);

        // Copy the command script into the container
        copyFileToContainer(
            Transferable.of(command.getBytes(StandardCharsets.UTF_8), 700),
            STARTER_SCRIPT
        );
    }

    /**
     * Sets the Kafka version, which image will be used.
     *
     * @param kafkaVersion the Kafka version
     * @return the StrimziProducerContainer instance
     */
    public StrimziProducerContainer withKafkaVersion(String kafkaVersion) {
        this.kafkaVersion = kafkaVersion;
        return self();
    }
}
