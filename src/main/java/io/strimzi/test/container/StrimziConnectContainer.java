/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.groupcdg.pitest.annotations.DoNotMutate;
import org.apache.logging.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.images.builder.Transferable;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public class StrimziConnectContainer extends GenericContainer<StrimziConnectContainer> {

    // class attributes
    private static final Logger LOGGER = LoggerFactory.getLogger(StrimziConnectContainer.class);

    /**
     * The file containing the startup script.
     */
    public static final String STARTER_SCRIPT = "/testcontainers_start.sh";

    /**
     * The file containing the Connect configuration
     */
    public static final String CONFIG_FILE = "/opt/kafka/config/connect.properties";

    /**
     * Default Kafka port
     */
    public static final int CONNECT_PORT = 8083;

    /**
     * The network alias.
     */
    protected static final String NETWORK_ALIAS = "connect";

    /**
     * Lazy image name provider
     */
    private final CompletableFuture<String> imageNameProvider;

    // instance attributes
    private String bootstrapServers;
    private Map<String, String> connectConfigurationMap;
    private final String kafkaVersion = KafkaVersionService.getInstance().latestRelease().getVersion();
    private boolean includeFileConnectors = true;

    /**
     * Image name is specified lazily automatically in {@link #doStart()} method
     */
    public StrimziConnectContainer() {
        this(new CompletableFuture<>());
    }

    /**
     * Image name is specified by {@code dockerImageName}
     *
     * @param dockerImageName specific docker image name provided by constructor parameter
     */
    public StrimziConnectContainer(String dockerImageName) {
        this(CompletableFuture.completedFuture(dockerImageName));
    }

    /**
     * Image name is lazily set in {@link #doStart()} method
     */
    private StrimziConnectContainer(CompletableFuture<String> imageName) {
        super(imageName);
        this.imageNameProvider = imageName;
        super.setNetwork(Network.SHARED);
        // exposing kafka port from the container
        super.setExposedPorts(Collections.singletonList(CONNECT_PORT));
        super.addEnv("LOG_DIR", "/tmp");
    }

    @Override
    @SuppressWarnings({"NPathComplexity", "CyclomaticComplexity"})
    @DoNotMutate
    protected void doStart() {
        if (bootstrapServers == null) {
            throw new IllegalStateException("Bootstrap servers must be configured using withBootstrapServers()");
        }
        if (!imageNameProvider.isDone()) {
            imageNameProvider.complete(KafkaVersionService.strimziTestContainerImageName(kafkaVersion));
        }

        super.withNetworkAliases(NETWORK_ALIAS);

        super.setCommand("sh", "-c", runStarterScript());
        super.doStart();
    }

    @Override
    @DoNotMutate
    public void stop() {
        super.stop();
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

    /**
     * Fluent method, which sets a waiting strategy to wait until the broker is ready.
     * <p>
     * This method waits for a log message in the broker log.
     * You can customize the strategy using {@link #waitingFor(WaitStrategy)}.
     *
     * @return StrimziConnectContainer instance
     */
    @DoNotMutate
    public StrimziConnectContainer waitForRunning() {
        super.waitingFor(Wait.forLogMessage(".*Finished starting connectors and tasks.*", 1));
        return this;
    }

    /**
     * The Connect REST API endpoint
     * @return the endpoint
     */
    public String restEndpoint() {
        return "http://" + getHost() + ":" + getMappedPort(CONNECT_PORT);
    }

    @Override
    @DoNotMutate
    protected void containerIsStarting(final InspectContainerResponse containerInfo, final boolean reused) {
        super.containerIsStarting(containerInfo, reused);

        LOGGER.info("Mapped port: {}", getMappedPort(CONNECT_PORT));

        final Properties defaultServerProperties = buildDefaultConnectProperties(bootstrapServers);
        final String serverPropertiesWithOverride = overrideProperties(defaultServerProperties, connectConfigurationMap);

        copyFileToContainer(
                Transferable.of(serverPropertiesWithOverride.getBytes(StandardCharsets.UTF_8)),
                CONFIG_FILE);

        String command = "#!/bin/bash \n";
        command += "bin/connect-distributed.sh " + CONFIG_FILE + " \n";

        LOGGER.info("Copying command to 'STARTER_SCRIPT' script.");

        copyFileToContainer(
                Transferable.of(command.getBytes(StandardCharsets.UTF_8), 700),
                STARTER_SCRIPT
        );
    }

    /**
     * Builds the default Kafka Connect properties.
     *
     * @param bootstrapServers the bootstrap servers
     * @return the default Connect properties
     */
    protected Properties buildDefaultConnectProperties(final String bootstrapServers) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty("group.id", "connect-cluster");
        properties.setProperty("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        properties.setProperty("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        properties.setProperty("key.converter.schemas.enable", "true");
        properties.setProperty("value.converter.schemas.enable", "true");
        properties.setProperty("offset.storage.topic", "connect-offsets");
        properties.setProperty("offset.storage.replication.factor", "1");
        properties.setProperty("config.storage.topic", "connect-configs");
        properties.setProperty("config.storage.replication.factor", "1");
        properties.setProperty("status.storage.topic", "connect-status");
        properties.setProperty("status.storage.replication.factor", "1");
        if (includeFileConnectors) {
            properties.setProperty("plugin.path", "/opt/kafka/libs/connect-file-" + kafkaVersion + ".jar");
        }
        return properties;
    }

    /**
     * Overrides the default Kafka Connect properties with the provided overrides.
     * If the overrides map is null or empty, it simply returns the default properties as a string.
     *
     * @param defaultProperties The default Kafka Connect properties.
     * @param overrides         The properties to override. Can be null.
     * @return A string representation of the combined Connect properties.
     */
    protected String overrideProperties(Properties defaultProperties, Map<String, String> overrides) {
        // Check if overrides are not null and not empty before applying them
        if (overrides != null && !overrides.isEmpty()) {
            overrides.forEach(defaultProperties::setProperty);
        }

        // Write properties to string
        StringWriter writer = new StringWriter();
        try {
            defaultProperties.store(writer, null);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to store Kafka server properties", e);
        }

        return writer.toString();
    }

    /**
     * Fluent method, which sets {@code connectConfigurationMap}.
     *
     * @param connectConfigurationMap kafka configuration
     * @return StrimziConnectContainer instance
     */
    public StrimziConnectContainer withConnectConfigurationMap(final Map<String, String> connectConfigurationMap) {
        this.connectConfigurationMap = connectConfigurationMap;
        return this;
    }

    /**
     * Fluent method to configure the bootstrap servers
     *
     * @param bootstrapServers the bootstrap servers
     * @return StrimziConnectContainer instance
     */
    public StrimziConnectContainer withBootstrapServers(final String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return self();
    }

    /**
     * Configures the Connect container to use the specified logging level for Connect logs.
     * <p>
     * This method generates a custom <code>connect-log4j.properties</code> file with the desired logging level
     * and copies it into the Connect container. By setting the logging level, you can control the verbosity
     * of Kafka's log output, which is useful for debugging or monitoring purposes.
     * </p>
     *
     * <b>Example Usage:</b>
     * <pre>{@code
     * StrimziConnectContainer connectContainer = new StrimziConnectContainer()
     *     .withConnectLog(Level.DEBUG)
     *     .start();
     * }</pre>
     *
     * @param level the desired {@link Level} of logging (e.g., DEBUG, INFO, WARN, ERROR)
     * @return the current instance of {@code StrimziConnectContainer} for method chaining
     */
    public StrimziConnectContainer withConnectLog(Level level) {
        String log4jConfig = "log4j.rootLogger=" + level.name() + ", stdout\n" +
                "log4j.appender.stdout=org.apache.log4j.ConsoleAppender\n" +
                "log4j.appender.stdout.layout=org.apache.log4j.PatternLayout\n" +
                "log4j.appender.stdout.layout.ConversionPattern=[%d] %p %m (%c)%n\n";

        // Copy the custom log4j.properties into the container
        this.withCopyToContainer(
                Transferable.of(log4jConfig.getBytes(StandardCharsets.UTF_8)),
                "/opt/kafka/config/connect-log4j.properties"
        );

        return self();
    }

    /**
     * Whether to include the FileStream connectors
     * @param includeFileConnectors Use false to not include the FileStream connectors
     * @return the current instance of {@code StrimziConnectContainer} for method chaining
     */
    public StrimziConnectContainer withIncludeFileConnectors(boolean includeFileConnectors) {
        this.includeFileConnectors = includeFileConnectors;
        return self();
    }
}
