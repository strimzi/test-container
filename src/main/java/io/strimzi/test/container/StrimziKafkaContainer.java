/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ContainerNetwork;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.apache.logging.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.toxiproxy.ToxiproxyContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.images.builder.Transferable;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * StrimziKafkaContainer is a single-node instance of Kafka using the image from quay.io/strimzi/kafka with the
 * given version.
 * <br><br>
 * Optionally, you can configure a {@code proxyContainer} to simulate network conditions (i.e. connection cut, latency).
 * This class uses {@code getBootstrapServers()} to build the {@code KAFKA_ADVERTISED_LISTENERS} configuration.
 * When {@code proxyContainer} is configured, the bootstrap URL returned by {@code getBootstrapServers()} contains the proxy host and port.
 * For this reason, Kafka clients will always pass through the proxy, even after refreshing cluster metadata.
 */
public class StrimziKafkaContainer extends GenericContainer<StrimziKafkaContainer> implements KafkaContainer {

    // class attributes
    private static final Logger LOGGER = LoggerFactory.getLogger(StrimziKafkaContainer.class);

    /**
     * The file containing the startup script.
     */
    public static final String STARTER_SCRIPT = "/testcontainers_start.sh";

    /**
     * Default Kafka port
     */
    public static final int KAFKA_PORT = 9092;

    /**
     * Default Kafka controller port
     */
    public static final int CONTROLLER_PORT = 9094;

    /**
     * Default Kafka controller external port (for host-accessible controller connections)
     */
    public static final int CONTROLLER_EXTERNAL_PORT = 9095;

    /**
     * Prefix for network aliases.
     */
    protected static final String NETWORK_ALIAS_PREFIX = "broker-";
    protected static final int INTER_BROKER_LISTENER_PORT = 9091;

    /**
     * Network alias for ToxiProxy container
     */
    protected static final String TOXIPROXY_NETWORK_ALIAS = "toxiproxy";

    /**
     * Base port for ToxiProxy listen ports (actual port = base + nodeId)
     */
    protected static final int TOXIPROXY_PORT_BASE = 8666;

    /**
     * Lazy image name provider
     */
    private final CompletableFuture<String> imageNameProvider;
    private final boolean enableBrokerContainerSlf4jLogging = Boolean.parseBoolean(
        System.getenv().getOrDefault("STRIMZI_TEST_CONTAINER_LOGGING_ENABLED", "false"));

    // instance attributes
    private int kafkaExposedPort;
    private int controllerExposedPort;
    private Map<String, String> kafkaConfigurationMap;
    private Integer nodeId;
    private String kafkaVersion;
    private Function<StrimziKafkaContainer, String> bootstrapServersProvider = c -> String.format("%s://%s:%s", getClientListenerProtocol(), getHost(), this.kafkaExposedPort);
    private String clusterId;
    private KafkaNodeRole nodeRole = KafkaNodeRole.COMBINED;
    private int fixedExposedPort;

    // proxy attributes
    private ToxiproxyContainer proxyContainer;
    private ToxiproxyClient toxiproxyClient;
    private Proxy proxy;

    /**
     * Defines the role of a Kafka listener.
     */
    protected enum ListenerRole {
        /** Client-facing listener for external connections */
        CLIENT,
        /** Inter-broker communication listener */
        INTER_BROKER,
        /** Controller communication listener (internal, Docker network) */
        CONTROLLER,
        /** Controller communication listener (external, host-accessible) */
        CONTROLLER_EXTERNAL
    }

    /**
     * Configuration for a Kafka listener with its name and role.
     */
    protected static class ListenerConfig {
        private final String name;
        private final ListenerRole role;

        ListenerConfig(String name, ListenerRole role) {
            this.name = name;
            this.role = role;
        }

        public String name() {
            return name;
        }

        public ListenerRole role() {
            return role;
        }
    }

    protected List<ListenerConfig> listeners = new ArrayList<>();

    // OAuth fields
    private boolean oauthEnabled;
    private String realm;
    private String clientId;
    private String clientSecret;
    private String oauthUri;
    private String usernameClaim;

    // OAuth over PLAIN
    private String saslUsername;
    private String saslPassword;

    private AuthenticationType authenticationType = AuthenticationType.NONE;

    // TLS field
    private CertAssembly certAssembly;

    // Log collection attributes
    private String logFilePath;

    /**
     * Image name is specified lazily automatically in {@link #doStart()} method
     */
    StrimziKafkaContainer() {
        this(new CompletableFuture<>());
    }

    /**
     * Image name is specified by {@code dockerImageName}
     *
     * @param dockerImageName specific docker image name provided by constructor parameter
     */
    StrimziKafkaContainer(String dockerImageName) {
        this(CompletableFuture.completedFuture(dockerImageName));
    }

    /**
     * Image name is lazily set in {@link #doStart()} method
     */
    private StrimziKafkaContainer(CompletableFuture<String> imageName) {
        super(imageName);
        this.imageNameProvider = imageName;
        // we need this shared network in case we deploy StrimziKafkaCluster which consist of `StrimziKafkaContainer`
        // instances and by default each container has its own network.
        super.setNetwork(Network.SHARED);
        super.addEnv("LOG_DIR", "/tmp");
    }

    @Override
    @DoNotMutate
    protected void doStart() {
        // Setup image name
        if (!this.imageNameProvider.isDone()) {
            this.imageNameProvider.complete(KafkaVersionService.strimziTestContainerImageName(this.kafkaVersion));
        }

        // Determine and set exposed ports based on node role and user configuration
        super.setExposedPorts(determineExposedPorts());

        // Setup network alias
        super.withNetworkAliases(NETWORK_ALIAS_PREFIX + this.nodeId);

        // Setup OAuth configuration
        setupOAuthConfiguration();

        // When TLS is enabled, the startup script waits for the keystore
        // to be distributed by the cluster before starting Kafka. The default
        // Testcontainers port-wait strategy would time out because the port isn't
        // listening yet. Use a log-based wait so the container is considered
        // "started" once the script reaches the keystore wait loop; the cluster
        // will then distribute certs and wait for quorum formation separately.
        if (this.certAssembly != null) {
            super.waitingFor(Wait.forLogMessage(".*Waiting for TLS keystore.*", 1)
                .withStartupTimeout(Duration.ofSeconds(30)));
        }

        // Setup logging
        if (this.enableBrokerContainerSlf4jLogging) {
            this.withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("StrimziKafkaContainer-" + this.nodeId)));
        }

        // Start the container
        super.setCommand("sh", "-c", runStarterScript());
        super.doStart();
    }

    /* test */ List<Integer> determineExposedPorts() {
        // Determine default ports based on node role
        List<Integer> portsToExpose = new ArrayList<>();
        if (this.nodeRole == KafkaNodeRole.CONTROLLER) {
            // Controller-only nodes only need the external controller port exposed to the host.
            // The internal CONTROLLER port (9094) is accessed via Docker network aliases.
            portsToExpose.add(CONTROLLER_EXTERNAL_PORT);
        } else if (this.nodeRole == KafkaNodeRole.BROKER) {
            // Broker-only nodes expose the Kafka client port
            portsToExpose.add(KAFKA_PORT);
        } else {
            // Combined-role nodes expose both client and controller ports
            portsToExpose.add(KAFKA_PORT);
            portsToExpose.add(CONTROLLER_PORT);
        }
        // Add any additional exposed ports
        portsToExpose.addAll(super.getExposedPorts());
        return portsToExpose;
    }

    @DoNotMutate
    private void setupOAuthConfiguration() {
        if (this.isOAuthEnabled()) {
            // Set OAuth environment variables (using properties does not propagate to System properties)
            this.addEnv("OAUTH_JWKS_ENDPOINT_URI", this.oauthUri + "/realms/" + this.realm + "/protocol/openid-connect/certs");
            this.addEnv("OAUTH_VALID_ISSUER_URI", this.oauthUri + "/realms/" + this.realm);
            this.addEnv("OAUTH_CLIENT_ID", this.clientId);
            this.addEnv("OAUTH_CLIENT_SECRET", this.clientSecret);
            this.addEnv("OAUTH_TOKEN_ENDPOINT_URI", this.oauthUri + "/realms/" + this.realm + "/protocol/openid-connect/token");
            this.addEnv("OAUTH_USERNAME_CLAIM", this.usernameClaim);
        }
    }

    @Override
    @DoNotMutate
    public void stop() {
        // Collect logs if log collection is enabled
        if (this.logFilePath != null) {
            collectLogs();
        }

        if (proxyContainer != null && proxyContainer.isRunning()) {
            proxyContainer.stop();
        }
        super.stop();
    }

    @DoNotMutate
    private void collectLogs() {
        try {
            // Determine the final log file path at runtime
            String finalLogPath = this.logFilePath;

            if (this.logFilePath.endsWith("/")) {
                String fileName;
                switch (this.nodeRole) {
                    case CONTROLLER:
                        fileName = "kafka-controller-" + this.nodeId + ".log";
                        break;
                    case BROKER:
                        fileName = "kafka-broker-" + this.nodeId + ".log";
                        break;
                    case COMBINED:
                    default:
                        fileName = "kafka-container-" + this.nodeId + ".log";
                        break;
                }
                finalLogPath = this.logFilePath + fileName;
            }

            LOGGER.info("Collecting logs to file: {}", finalLogPath);
            final String logs = getLogs();
            final Path logPath = Paths.get(finalLogPath);

            // Create directories if they don't exist
            if (logPath.getParent() != null) {
                Files.createDirectories(logPath.getParent());
            }

            writeDataToFile(finalLogPath, logs);

            LOGGER.info("Successfully collected logs to: {}", logPath.toAbsolutePath());
        } catch (IOException e) {
            LOGGER.error("Failed to collect logs to file: {}", this.logFilePath, e);
            throw new RuntimeException("Failed to collect logs to file: " + this.logFilePath, e);
        }
    }

    /**
     * Method that writes data to file (on path, specified by {@param fullFilePath}.
     *
     * @param fullFilePath full path to file
     * @param data         data which should be written to file
     */
    @DoNotMutate
    private void writeDataToFile(String fullFilePath, String data) {
        if (data != null && !data.isEmpty()) {
            try {
                Files.writeString(Paths.get(fullFilePath), data, StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new RuntimeException(
                    String.format("Failed to write to the %s file due to: %s", fullFilePath, e.getMessage())
                );
            }
        }
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
     * Fluent method, which sets a waiting strategy to wait until the node is ready.
     * <p>
     * This method waits for a log message in the node log. The specific message depends on the node role:
     * - Broker and combined-role nodes wait for "Transitioning from RECOVERY to RUNNING"
     * - Controller-only nodes wait for "Kafka Server started"
     * You can customize the strategy using {@link #waitingFor(WaitStrategy)}.
     *
     * @return StrimziKafkaContainer instance
     */
    @DoNotMutate
    public StrimziKafkaContainer waitForRunning() {
        if (this.nodeRole == KafkaNodeRole.CONTROLLER) {
            // Controller-only nodes don't have the broker lifecycle, so wait for server startup
            super.waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1));
        } else {
            // Broker and combined-role nodes wait for broker lifecycle transition
            super.waitingFor(Wait.forLogMessage(".*Transitioning from RECOVERY to RUNNING.*", 1));
        }
        return this;
    }

    @Override
    @DoNotMutate
    protected void containerIsStarting(final InspectContainerResponse containerInfo, final boolean reused) {
        super.containerIsStarting(containerInfo, reused);

        if (this.nodeRole.isBroker()) {
            this.kafkaExposedPort = getMappedPort(KAFKA_PORT);
            LOGGER.info("Mapped Kafka port: {}", kafkaExposedPort);
        }

        if (this.nodeRole.isController()) {
            if (this.nodeRole == KafkaNodeRole.CONTROLLER) {
                // Controller-only: external access uses CONTROLLER_EXTERNAL_PORT
                this.controllerExposedPort = getMappedPort(CONTROLLER_EXTERNAL_PORT);
            } else {
                // Combined-mode: external access uses CONTROLLER_PORT (no CONTROLLER_EXTERNAL)
                this.controllerExposedPort = getMappedPort(CONTROLLER_PORT);
            }
            LOGGER.info("Mapped controller port: {}", controllerExposedPort);
        }

        if (this.nodeId == null) {
            throw new IllegalStateException("Node ID must be set using withNodeId()");
        }

        final String[] listenersConfig = this.buildListenersConfig(containerInfo);
        final Properties defaultServerProperties = this.buildDefaultServerProperties(
            listenersConfig[0],
            listenersConfig[1]);
        final String serverPropertiesWithOverride = this.overrideProperties(defaultServerProperties, this.kafkaConfigurationMap);

        // copy override file to the container
        copyFileToContainer(Transferable.of(serverPropertiesWithOverride.getBytes(StandardCharsets.UTF_8)), "/opt/kafka/config/kraft/server.properties");

        if (this.clusterId == null) {
            this.clusterId = this.randomUuid();
            LOGGER.info("New `cluster.id` has been generated: {}", this.clusterId);
        }

        final String command = buildStartupCommand();

        LOGGER.info("Copying command to 'STARTER_SCRIPT' script.");

        copyFileToContainer(
                Transferable.of(command.getBytes(StandardCharsets.UTF_8), 700),
                STARTER_SCRIPT
        );
    }

    /**
     * Builds the startup script command based on the current configuration.
     * When TLS is enabled on a broker, the script waits for the keystore file
     * to be distributed by {@link StrimziKafkaCluster} before starting Kafka.
     *
     * @return the startup script content
     */
    @DoNotMutate
    private String buildStartupCommand() {
        StringBuilder command = new StringBuilder("#!/bin/bash \n");

        if (this.certAssembly != null) {
            command.append("bin/kafka-storage.sh format -t=\"").append(this.clusterId)
                .append("\" -c /opt/kafka/config/kraft/server.properties \n");
            command.append("echo 'Waiting for TLS keystore to be distributed...' \n");
            command.append("while [ ! -f ").append(CertAssembly.BROKER_KEYSTORE_PATH).append(" ]; do sleep 0.1; done \n");
            command.append("bin/kafka-server-start.sh /opt/kafka/config/kraft/server.properties \n");
            return command.toString();
        }

        command.append("bin/kafka-storage.sh format -t=\"").append(this.clusterId)
            .append("\" -c /opt/kafka/config/kraft/server.properties \n");
        command.append("bin/kafka-server-start.sh /opt/kafka/config/kraft/server.properties \n");
        return command.toString();
    }

    protected String extractListenerName(String bootstrapServers) {
        // extract listener name from given bootstrap servers
        String[] strings = bootstrapServers.split(":");
        if (strings.length < 3) {
            throw new IllegalArgumentException("The configured boostrap servers '" + bootstrapServers +
                    "' must be prefixed with a listener name.");
        }
        return strings[0];
    }

    /**
     * Builds the listener configurations for the Kafka broker based on the container's network settings.
     *
     * @param containerInfo Container network information.
     * @return An array containing:
     *          The 'listeners' configuration string.
     *          The 'advertised.listeners' configuration string.
     */
    protected String[] buildListenersConfig(final InspectContainerResponse containerInfo) {
        final Collection<ContainerNetwork> networks = containerInfo.getNetworkSettings().getNetworks().values();
        final List<String> advertisedListenersNames = new ArrayList<>();
        final StringBuilder kafkaListeners = new StringBuilder();
        final StringBuilder advertisedListeners = new StringBuilder();

        String bsListenerName = null;
        String bootstrapServers = null;

        // Only get bootstrap servers for broker nodes
        if (this.nodeRole.isBroker()) {
            bootstrapServers = getBootstrapServers();
            bsListenerName = extractListenerName(bootstrapServers);
        }

        // Only add client listener for broker nodes
        if (this.nodeRole.isBroker() && bsListenerName != null) {
            // add first PLAINTEXT listener
            advertisedListeners.append(bootstrapServers);
            kafkaListeners.append(bsListenerName).append(":").append("//").append("0.0.0.0").append(":").append(KAFKA_PORT).append(",");
            this.listeners.add(new ListenerConfig(bsListenerName, ListenerRole.CLIENT));
        }

        int listenerNumber = 1;
        int portNumber = INTER_BROKER_LISTENER_PORT;

        // Only add inter-broker listeners for nodes that act as brokers
        if (this.nodeRole.isBroker()) {
            // TODO: refactor this... the loop iterates over networks but no longer uses the network variable
            //  (i.e., we use the network alias instead of the container IP for TLS SAN compatibility).
            //  We can replace this with single inter-broker listener since the alias is the same
            //  regardless of the number of networks.
            // https://github.com/strimzi/test-container/issues/192
            for (ContainerNetwork network : networks) {
                String advertisedName = "BROKER" + listenerNumber;
                advertisedListeners.append(",")
                    .append(advertisedName)
                    .append("://")
                    .append(NETWORK_ALIAS_PREFIX).append(this.nodeId)
                    .append(":")
                    .append(portNumber);
                advertisedListenersNames.add(advertisedName);
                listenerNumber++;
                portNumber--;
            }
        }

        portNumber = INTER_BROKER_LISTENER_PORT;

        // Only add inter-broker listeners for nodes that act as brokers
        if (this.nodeRole.isBroker()) {
            // configure listeners
            for (String listener : advertisedListenersNames) {
                kafkaListeners
                    .append(listener)
                    .append("://0.0.0.0:")
                    .append(portNumber)
                    .append(",");
                this.listeners.add(new ListenerConfig(listener, ListenerRole.INTER_BROKER));
                portNumber--;
            }
        }

        // Only add controller listener for nodes that act as controllers
        addControllerListeners(kafkaListeners, advertisedListeners);

        LOGGER.info("This is all advertised listeners for Kafka {}", advertisedListeners);

        String listeners = kafkaListeners.toString();
        if (listeners.endsWith(",")) {
            listeners = listeners.substring(0, listeners.length() - 1);
        }

        return new String[] {
            listeners,
            advertisedListeners.toString()
        };
    }

    /**
     * Adds controller listeners for controller nodes.
     * Controller-only nodes get both CONTROLLER (internal) and CONTROLLER_EXTERNAL (host-accessible) listeners.
     * Combined-mode nodes only get the CONTROLLER listener.
     *
     * @param kafkaListeners StringBuilder for listeners configuration
     * @param advertisedListeners StringBuilder for advertised.listeners configuration
     */
    private void addControllerListeners(StringBuilder kafkaListeners, StringBuilder advertisedListeners) {
        if (!this.nodeRole.isController()) {
            return;
        }

        // Internal controller listener (Docker network alias) -- all controller nodes
        if (advertisedListeners.length() > 0) {
            advertisedListeners.append(",");
        }
        advertisedListeners.append(getNetworkBootstrapControllers());
        kafkaListeners.append("CONTROLLER").append("://0.0.0.0:").append(StrimziKafkaContainer.CONTROLLER_PORT);
        this.listeners.add(new ListenerConfig("CONTROLLER", ListenerRole.CONTROLLER));

        // External controller listener -- controller-only nodes only
        // (Kafka forbids controller listeners in advertised.listeners for combined-mode nodes)
        if (this.nodeRole == KafkaNodeRole.CONTROLLER) {
            advertisedListeners.append(",");
            advertisedListeners.append(getBootstrapControllers());
            kafkaListeners.append(",").append("CONTROLLER_EXTERNAL").append("://0.0.0.0:").append(StrimziKafkaContainer.CONTROLLER_EXTERNAL_PORT);
            this.listeners.add(new ListenerConfig("CONTROLLER_EXTERNAL", ListenerRole.CONTROLLER_EXTERNAL));
        }
    }

    /**
     * In order to avoid any compile dependency on kafka-clients' <code>Uuid</code> specific class,
     * we implement our own uuid generator by replicating the Kafka's base64 encoded uuid generation logic.
     */
    @DoNotMutate
    private String randomUuid() {
        final UUID metadataTopicIdInternal = new UUID(0L, 1L);
        final UUID zeroIdImpactInternal = new UUID(0L, 0L);
        UUID uuid;
        for (uuid = UUID.randomUUID(); uuid.equals(metadataTopicIdInternal) || uuid.equals(zeroIdImpactInternal); uuid = UUID.randomUUID()) {
        }

        final ByteBuffer uuidBytes = ByteBuffer.wrap(new byte[16]);
        uuidBytes.putLong(uuid.getMostSignificantBits());
        uuidBytes.putLong(uuid.getLeastSignificantBits());
        final byte[] uuidBytesArray = uuidBytes.array();

        return Base64.getUrlEncoder().withoutPadding().encodeToString(uuidBytesArray);
    }

    /**
     * Builds the default Kafka server properties.
     *
     * @param listeners                   the listeners configuration
     * @param advertisedListeners         the advertised listeners configuration
     * @return the default server properties
     */
    protected Properties buildDefaultServerProperties(final String listeners,
                                                    final String advertisedListeners) {
        // Default properties for server.properties
        Properties properties = new Properties();

        // Common settings for both KRaft and non-KRaft modes
        // configure listeners
        properties.setProperty("listeners", listeners);

        if (this.nodeRole.isBroker()) {
            properties.setProperty("inter.broker.listener.name", "BROKER1");
            properties.setProperty("advertised.listeners", advertisedListeners);
        } else {
            // Controller-only nodes should only have controller listeners in advertised.listeners
            extractControllerListener(properties, advertisedListeners);
        }

        String protocol = this.certAssembly != null ? "SSL" : "PLAINTEXT";
        String securityProtocolMap = this.configureListenerSecurityProtocolMap(protocol);
        // Ensure CONTROLLER mapping exists on ALL nodes when controller.listener.names is set
        securityProtocolMap = ensureControllerMapping(securityProtocolMap, protocol);

        properties.setProperty("listener.security.protocol.map", securityProtocolMap);
        LOGGER.info("Node {} ({}) - Security protocol map: {}, TLS enabled: {}", this.nodeId, this.nodeRole, securityProtocolMap, this.certAssembly != null);

        setCommonServerProperties(properties);
        setKRaftProperties(properties);
        configureAuthentication(properties);
        configureTls(properties);

        return properties;
    }

    /* test */ void extractControllerListener(Properties properties, String advertisedListeners) {
        if (advertisedListeners == null || advertisedListeners.isEmpty()) {
            return;
        }
        String[] parts = advertisedListeners.split(",");
        String controllerListeners = Arrays.stream(parts)
            .map(String::trim)
            .filter(p -> p.startsWith("CONTROLLER://") || p.startsWith("CONTROLLER_EXTERNAL://"))
            .collect(Collectors.joining(","));
        if (!controllerListeners.isEmpty()) {
            properties.setProperty("advertised.listeners", controllerListeners);
        }
    }

    private void setCommonServerProperties(Properties properties) {
        properties.setProperty("num.network.threads", "3");
        properties.setProperty("num.io.threads", "8");
        properties.setProperty("socket.send.buffer.bytes", "102400");
        properties.setProperty("socket.receive.buffer.bytes", "102400");
        properties.setProperty("socket.request.max.bytes", "104857600");
        properties.setProperty("log.dirs", "/tmp/default-log-dir");
        properties.setProperty("num.partitions", "1");
        properties.setProperty("num.recovery.threads.per.data.dir", "1");
        properties.setProperty("offsets.topic.replication.factor", "1");
        properties.setProperty("transaction.state.log.replication.factor", "1");
        properties.setProperty("transaction.state.log.min.isr", "1");
        properties.setProperty("log.retention.hours", "168");
        properties.setProperty("log.retention.check.interval.ms", "300000");
    }

    private void setKRaftProperties(Properties properties) {
        properties.setProperty("process.roles", this.nodeRole.getProcessRoles());
        properties.setProperty("node.id", String.valueOf(this.nodeId));

        // Controller-only nodes have dual controller listeners (CONTROLLER + CONTROLLER_EXTERNAL)
        // Combined and broker-only nodes use only CONTROLLER
        if (this.nodeRole == KafkaNodeRole.CONTROLLER) {
            properties.setProperty("controller.listener.names", "CONTROLLER,CONTROLLER_EXTERNAL");
        } else {
            properties.setProperty("controller.listener.names", "CONTROLLER");
        }

        // For standalone containers (not part of a cluster), set default quorum voters
        if (this.kafkaConfigurationMap == null || !this.kafkaConfigurationMap.containsKey("controller.quorum.voters")) {
            if (this.nodeRole == KafkaNodeRole.COMBINED) {
                properties.setProperty("controller.quorum.voters",
                    String.format("%d@%s%d:%d", this.nodeId, NETWORK_ALIAS_PREFIX, this.nodeId, StrimziKafkaContainer.CONTROLLER_PORT));
            }
        }
    }

    private void configureAuthentication(Properties properties) {
        if (this.authenticationType != AuthenticationType.NONE) {
            switch (this.authenticationType) {
                case OAUTH_OVER_PLAIN:
                    validateOAuthAndConfigure(properties, this::configureOAuthOverPlain);
                    break;
                case OAUTH_BEARER:
                    validateOAuthAndConfigure(properties, this::configureOAuthBearer);
                    break;
                case SCRAM_SHA_256:
                case SCRAM_SHA_512:
                case GSSAPI:
                default:
                    throw new IllegalStateException("Unsupported authentication type: " + this.authenticationType);
            }
        }
    }

    /**
     * Configures per-listener TLS settings if TLS is enabled.
     * Client-facing listeners use the broker keystore/truststore,
     * while inter-broker and controller listeners use separate
     * internal keystore/truststore.
     *
     * @param properties The Kafka server properties to configure.
     */
    private void configureTls(Properties properties) {
        if (this.certAssembly == null) {
            return;
        }

        // Broker-only nodes need CONTROLLER listener SSL config for outbound
        // Raft connections to controllers, even though they do not bind the
        // CONTROLLER listener themselves.
        boolean hasControllerListener = this.listeners.stream()
            .anyMatch(l -> l.role() == ListenerRole.CONTROLLER);
        if (!hasControllerListener) {
            this.listeners.add(new ListenerConfig("CONTROLLER", ListenerRole.CONTROLLER));
        }

        for (ListenerConfig listener : this.listeners) {
            boolean isInternal =
                listener.role() == ListenerRole.INTER_BROKER
                || listener.role() == ListenerRole.CONTROLLER;

            String password = isInternal
                ? this.certAssembly.getInternalPassword()
                : this.certAssembly.getBrokerPassword();
            String ksPath = isInternal
                ? CertAssembly.INTERNAL_KEYSTORE_PATH
                : CertAssembly.BROKER_KEYSTORE_PATH;
            String tsPath = isInternal
                ? CertAssembly.INTERNAL_TRUSTSTORE_PATH
                : CertAssembly.BROKER_TRUSTSTORE_PATH;

            String prefix = "listener.name." + listener.name().toLowerCase(Locale.ROOT) + ".ssl.";

            properties.setProperty(prefix + "keystore.location", ksPath);
            properties.setProperty(prefix + "keystore.password", password);
            properties.setProperty(prefix + "keystore.type", "PKCS12");
            properties.setProperty(prefix + "key.password", password);
            properties.setProperty(prefix + "truststore.location", tsPath);
            properties.setProperty(prefix + "truststore.password", password);
            properties.setProperty(prefix + "truststore.type", "PKCS12");
            properties.setProperty(prefix + "client.auth", "required");
        }
    }

    private void validateOAuthAndConfigure(Properties properties, Consumer<Properties> configurer) {
        if (this.isOAuthEnabled()) {
            configurer.accept(properties);
        } else {
            throw new IllegalStateException("OAuth2 is not enabled: " + this.oauthEnabled);
        }
    }

    /**
     * Configures OAuth over PLAIN authentication in the provided properties.
     *
     * @param properties The Kafka server properties to configure.
     */
    protected void configureOAuthOverPlain(Properties properties) {
        properties.setProperty("sasl.enabled.mechanisms", "PLAIN");
        properties.setProperty("sasl.mechanism.inter.broker.protocol", "PLAIN");

        // Determine protocol based on TLS setting
        String saslProtocol = this.certAssembly != null ? "SASL_SSL" : "SASL_PLAINTEXT";
        String securityProtocolMap = this.configureListenerSecurityProtocolMap(saslProtocol);
        securityProtocolMap = ensureControllerMapping(securityProtocolMap, saslProtocol);
        properties.setProperty("listener.security.protocol.map", securityProtocolMap);
        
        properties.setProperty("sasl.mechanism.controller.protocol", "PLAIN");
        properties.setProperty("principal.builder.class", "io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder");

        // Construct the JAAS configuration with configurable username and password
        final String jaasConfig = String.format(
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
            this.saslUsername,
            this.saslPassword
        );
        // Callback handler classes
        final String callbackHandler = "io.strimzi.kafka.oauth.server.plain.JaasServerOauthOverPlainValidatorCallbackHandler";

        for (ListenerConfig listener : this.listeners) {
            String listenerName = listener.name().toLowerCase(Locale.ROOT);
            properties.setProperty("listener.name." + listenerName + ".plain.sasl.jaas.config", jaasConfig);
            properties.setProperty("listener.name." + listenerName + ".plain.sasl.server.callback.handler.class", callbackHandler);
        }
    }

    /**
     * Configures OAuth Bearer authentication in the provided properties.
     *
     * @param properties The Kafka server properties to configure.
     */
    protected void configureOAuthBearer(Properties properties) {
        properties.setProperty("sasl.enabled.mechanisms", "OAUTHBEARER");
        properties.setProperty("sasl.mechanism.inter.broker.protocol", "OAUTHBEARER");

        // Determine protocol based on TLS setting
        String saslProtocol = this.certAssembly != null ? "SASL_SSL" : "SASL_PLAINTEXT";
        String securityProtocolMap = this.configureListenerSecurityProtocolMap(saslProtocol);
        securityProtocolMap = ensureControllerMapping(securityProtocolMap, saslProtocol);

        properties.setProperty("listener.security.protocol.map", securityProtocolMap);
        
        properties.setProperty("sasl.mechanism.controller.protocol", "OAUTHBEARER");
        properties.setProperty("principal.builder.class", "io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder");

        // Construct JAAS configuration for OAUTHBEARER
        final String jaasConfig = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required ;";
        // Define Callback Handlers for OAUTHBEARER
        final String serverCallbackHandler = "io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler";
        final String clientSideCallbackHandler = "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler";

        for (final ListenerConfig listener : this.listeners) {
            String listenerName = listener.name().toLowerCase(Locale.ROOT);
            properties.setProperty("listener.name." + listenerName + ".oauthbearer.sasl.jaas.config", jaasConfig);
            properties.setProperty("listener.name." + listenerName + ".oauthbearer.sasl.server.callback.handler.class", serverCallbackHandler);
            properties.setProperty("listener.name." + listenerName + ".oauthbearer.sasl.login.callback.handler.class", clientSideCallbackHandler);
        }
    }

    /**
     * Configures the listener.security.protocol.map property with the same protocol for all listeners.
     *
     * @param securityProtocol The security protocol for all listeners (e.g., PLAINTEXT, SSL, SASL_SSL).
     * @return The listener.security.protocol.map configuration string.
     */
    protected String configureListenerSecurityProtocolMap(String securityProtocol) {
        return this.listeners.stream()
            .map(listener -> listener.name() + ":" + securityProtocol)
            .collect(Collectors.joining(","));
    }

    /**
     * Ensures {@code CONTROLLER:<protocol>} is present in the given
     * {@code listener.security.protocol.map}.
     *
     * @param securityProtocolMap current map
     * @param controllerProtocol  protocol for CONTROLLER (e.g. PLAINTEXT, SASL_PLAINTEXT)
     * @return securityProtocolMapWithControllerMapping string including CONTROLLER mapping
     */
    private String ensureControllerMapping(String securityProtocolMap, String controllerProtocol) {
        String result = (securityProtocolMap == null) ? "" : securityProtocolMap.trim();

        final String[] requiredMappings = (this.nodeRole == KafkaNodeRole.CONTROLLER)
            ? new String[]{"CONTROLLER:", "CONTROLLER_EXTERNAL:"}
            : new String[]{"CONTROLLER:"};

        for (final String prefix : requiredMappings) {
            if (!result.contains(prefix)) {
                result = result.isEmpty() ? prefix + controllerProtocol : result + "," + prefix + controllerProtocol;
            }
        }

        return result;
    }

    /**
     * Determines the security protocol for the client listener based on TLS and authentication settings.
     *
     * @return the security protocol string
     */
    /* test */ String getClientListenerProtocol() {
        boolean hasSasl = this.authenticationType != AuthenticationType.NONE;
        if (this.certAssembly != null && hasSasl) {
            return "SASL_SSL";
        } else if (this.certAssembly != null) {
            return "SSL";
        } else if (hasSasl) {
            return "SASL_PLAINTEXT";
        } else {
            return "PLAINTEXT";
        }
    }

    /**
     * Overrides the default Kafka server properties with the provided overrides.
     * If the overrides map is null or empty, it simply returns the default properties as a string.
     *
     * @param defaultProperties The default Kafka server properties.
     * @param overrides         The properties to override. Can be null.
     * @return A string representation of the combined server properties.
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
     * Retrieves the bootstrap servers URL for Kafka clients.
     *
     * @return the bootstrap servers URL
     */
    @Override
    @DoNotMutate
    public String getBootstrapServers() {
        // Controller-only nodes don't provide bootstrap servers
        if (this.nodeRole == KafkaNodeRole.CONTROLLER) {
            throw new UnsupportedOperationException("Controller-only nodes do not provide bootstrap servers. Use broker or combined-role nodes for client connections.");
        }
        
        if (proxyContainer != null) {
            initializeProxy();
            // Return the host-accessible proxy address
            final int mappedPort = proxyContainer.getMappedPort(TOXIPROXY_PORT_BASE + this.nodeId);
            return String.format("PLAINTEXT://%s:%d", proxyContainer.getHost(), mappedPort);
        }
        return bootstrapServersProvider.apply(this);
    }

    /**
     * Get the bootstrap servers that containers on the same network should use to connect
     * @return a comma separated list of Kafka bootstrap servers
     */
    public String getNetworkBootstrapServers() {
        // Controller-only nodes don't provide bootstrap servers
        if (this.nodeRole == KafkaNodeRole.CONTROLLER) {
            throw new UnsupportedOperationException("Controller-only nodes do not provide bootstrap servers. Use broker or combined-role nodes for client connections.");
        }

        if (proxyContainer != null) {
            initializeProxy();
            // Return the network-accessible proxy address (using toxiproxy network alias)
            final int listenPort = TOXIPROXY_PORT_BASE + this.nodeId;
            return String.format("PLAINTEXT://%s:%d", TOXIPROXY_NETWORK_ALIAS, listenPort);
        }
        return String.format("PLAINTEXT://%s%d:%d", NETWORK_ALIAS_PREFIX, nodeId, INTER_BROKER_LISTENER_PORT);
    }

    /**
     * Retrieves the bootstrap controllers URL for admin clients that need to connect to controllers.
     * This is required for certain admin operations in KRaft mode, such as describing the cluster
     * or performing controller-specific operations.
     *
     * @return the bootstrap controllers URL
     * @throws UnsupportedOperationException if this node doesn't have controller role
     */
    @Override
    @DoNotMutate
    public String getBootstrapControllers() {
        // Only controller nodes can provide controller endpoints
        if (!this.nodeRole.isController()) {
            throw new UnsupportedOperationException("Broker-only nodes do not provide controller endpoints. Use controller or combined-role nodes for controller connections.");
        }

        // Controller-only nodes use CONTROLLER_EXTERNAL listener name (external access)
        // Combined-mode nodes keep CONTROLLER listener name
        final String listenerName = (this.nodeRole == KafkaNodeRole.CONTROLLER) ? "CONTROLLER_EXTERNAL" : "CONTROLLER";

        if (proxyContainer != null) {
            initializeProxy();
            // Return the host-accessible proxy address
            final int listenPort = TOXIPROXY_PORT_BASE + this.nodeId;
            final int mappedPort = proxyContainer.getMappedPort(listenPort);
            return String.format("%s://%s:%d", listenerName, proxyContainer.getHost(), mappedPort);
        }
        return String.format("%s://%s:%d", listenerName, getHost(), this.controllerExposedPort);
    }

    /**
     * Get the bootstrap controllers that containers on the same network should use to connect to controllers
     * @return a comma separated list of Kafka controller endpoints
     * @throws UnsupportedOperationException if this node doesn't have controller role
     */
    public String getNetworkBootstrapControllers() {
        // Only controller nodes can provide controller endpoints
        if (!this.nodeRole.isController()) {
            throw new UnsupportedOperationException("Broker-only nodes do not provide controller endpoints. Use controller or combined-role nodes for controller connections.");
        }

        if (proxyContainer != null) {
            initializeProxy();
            // Return the network-accessible proxy address (using toxiproxy network alias)
            final int listenPort = TOXIPROXY_PORT_BASE + this.nodeId;
            return String.format("CONTROLLER://%s:%d", TOXIPROXY_NETWORK_ALIAS, listenPort);
        }
        return String.format("CONTROLLER://%s%d:%d", NETWORK_ALIAS_PREFIX, nodeId, StrimziKafkaContainer.CONTROLLER_PORT);
    }

    /**
     * Get the cluster id. This is only supported for KRaft containers.
     * @return The cluster id.
     */
    public String getClusterId() {
        return clusterId;
    }

    /**
     * Fluent method, which sets {@code kafkaConfigurationMap}.
     *
     * @param kafkaConfigurationMap kafka configuration
     * @return StrimziKafkaContainer instance
     */
    public StrimziKafkaContainer withKafkaConfigurationMap(final Map<String, String> kafkaConfigurationMap) {
        this.kafkaConfigurationMap = kafkaConfigurationMap;
        return this;
    }


    /**
     * Fluent method that sets the node ID.
     *
     * @param nodeId the node ID
     * @return {@code StrimziKafkaContainer} instance
     */
    public StrimziKafkaContainer withNodeId(final int nodeId) {
        this.nodeId = nodeId;
        return self();
    }

    /**
     * Fluent method, which sets {@code kafkaVersion}.
     *
     * @param kafkaVersion kafka version
     * @return StrimziKafkaContainer instance
     */
    public StrimziKafkaContainer withKafkaVersion(final String kafkaVersion) {
        this.kafkaVersion = kafkaVersion;
        return self();
    }

    /**
     * Fluent method, which sets fixed exposed port.
     *
     * @param fixedPort fixed port to expose
     * @return StrimziKafkaContainer instance
     */
    public StrimziKafkaContainer withPort(final int fixedPort) {
        this.fixedExposedPort = fixedPort;

        if (fixedExposedPort <= 0) {
            throw new IllegalArgumentException("The fixed Kafka port must be greater than 0");
        }
        addFixedExposedPort(fixedExposedPort, KAFKA_PORT);
        return self();
    }

    /**
     * Fluent method, assign provider for overriding bootstrap servers string
     *
     * @param provider provider function for bootstrapServers string
     * @return StrimziKafkaContainer instance
     */
    public StrimziKafkaContainer withBootstrapServers(final Function<StrimziKafkaContainer, String> provider) {
        this.bootstrapServersProvider = provider;
        return self();
    }

    /**
     * Fluent method, which sets a proxy container.
     * This container allows to create a TCP proxy between test code and Kafka broker.
     *
     * Every Kafka broker request will pass through the proxy where you can simulate
     * network conditions (i.e. connection cut, latency).
     *
     * @param proxyContainer Proxy container
     * @return StrimziKafkaContainer instance
     */
    public StrimziKafkaContainer withProxyContainer(final ToxiproxyContainer proxyContainer) {
        if (proxyContainer != null) {
            this.proxyContainer = proxyContainer;
            // Note: The network is set by the caller (e.g., StrimziKafkaCluster)
            // to ensure all containers are on the same network
            proxyContainer.setNetworkAliases(List.of(TOXIPROXY_NETWORK_ALIAS));
        }
        return self();
    }

    /**
     * Fluent method to configure OAuth settings.
     *
     * @param realm                 the realm
     * @param clientId              the OAuth client ID
     * @param clientSecret          the OAuth client secret
     * @param oauthUri              the OAuth URI
     * @param usernameClaim         the preferred username claim for OAuth
     * @return {@code StrimziKafkaContainer} instance
     */
    public StrimziKafkaContainer withOAuthConfig(final String realm,
                                                 final String clientId,
                                                 final String clientSecret,
                                                 final String oauthUri,
                                                 final String usernameClaim) {
        this.oauthEnabled = true;
        this.realm = realm;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.oauthUri = oauthUri;
        this.usernameClaim = usernameClaim;
        return self();
    }

    /**
     * Sets the authentication type for the Kafka container.
     *
     * @param authType The authentication type to enable.
     * @return StrimziKafkaContainer instance for method chaining.
     */
    public StrimziKafkaContainer withAuthenticationType(AuthenticationType authType) {
        if (authType != null) {
            this.authenticationType = authType;
        }
        return self();
    }

    /**
     * Fluent method to set the SASL PLAIN mechanism's username.
     *
     * @param saslUsername The desired SASL username.
     * @return StrimziKafkaContainer instance for method chaining.
     */
    public StrimziKafkaContainer withSaslUsername(String saslUsername) {
        this.saslUsername = Utils.requireNonBlank(saslUsername, "SASL username");
        return self();
    }

    /**
     * Fluent method to set the SASL PLAIN mechanism's password.
     *
     * @param saslPassword The desired SASL password.
     * @return StrimziKafkaContainer instance for method chaining.
     */
    public StrimziKafkaContainer withSaslPassword(String saslPassword) {
        this.saslPassword = Utils.requireNonBlank(saslPassword, "SASL password");
        return self();
    }

    /**
     * Enables TLS for client connections on this container.
     * Called by {@link StrimziKafkaCluster} to configure TLS.
     * The cluster is responsible for generating/providing certificates
     * and copying them to the container.
     *
     * @param certAssembly the certificate assembly containing keystore/truststore password and lifecycle operations
     */
    /* test */ StrimziKafkaContainer enableTls(CertAssembly certAssembly) {
        if (certAssembly == null) {
            throw new IllegalArgumentException("CertAssembly cannot be null.");
        }
        this.certAssembly = certAssembly;
        return self();
    }

    protected StrimziKafkaContainer withClusterId(String clusterId) {
        this.clusterId = clusterId;
        return self();
    }

    /**
     * Fluent method to set the Kafka node role.
     *
     * @param nodeRole the role this node should play in the cluster
     * @return StrimziKafkaContainer instance for method chaining
     */
    public StrimziKafkaContainer withNodeRole(KafkaNodeRole nodeRole) {
        this.nodeRole = nodeRole;
        return self();
    }

    /**
     * Configures the Kafka container to use the specified logging level for Kafka logs
     * and the {@code io.strimzi} logger.
     * <p>
     * This method generates a custom <code>log4j2.yaml</code> file with the desired logging level
     * and copies it into the Kafka container. By setting the logging level, you can control the verbosity
     * of Kafka's log output, which is useful for debugging or monitoring purposes.
     * </p>
     *
     * <b>Example Usage:</b>
     * <pre>{@code
     * StrimziKafkaContainer kafkaContainer = new StrimziKafkaContainer()
     *     .withKafkaLog(Level.DEBUG)
     *     .start();
     * }</pre>
     *
     * @param level the desired {@link Level} of logging (e.g., DEBUG, INFO, WARN, ERROR)
     * @return the current instance of {@code StrimziKafkaContainer} for method chaining
     */
    public StrimziKafkaContainer withKafkaLog(Level level) {
        final String log4j2Yaml =
            "Configuration:\n" +
                "  Properties:\n" +
                "    Property:\n" +
                "      - name: logPattern\n" +
                "        value: \"[%d] %p %m (%c)%n\"\n" +
                "  Appenders:\n" +
                "    Console:\n" +
                "      name: STDOUT\n" +
                "      PatternLayout:\n" +
                "        pattern: \"${logPattern}\"\n" +
                "  Loggers:\n" +
                "    Root:\n" +
                "      level: " + level.name() + "\n" +
                "      AppenderRef:\n" +
                "        - ref: STDOUT\n" +
                "    Logger:\n" +
                "      - name: io.strimzi\n" +
                "        level: " + level.name() + "\n";

        // Copy the custom log4j2.properties into the container
        this.withCopyToContainer(
            Transferable.of(log4j2Yaml.getBytes(StandardCharsets.UTF_8)),
            "/opt/kafka/config/log4j2.yaml"
        );

        return self();
    }

    /**
     * Fluent method to enable log collection with a default log file path.
     * The actual filename is determined at runtime when logs are collected.
     *
     * @return StrimziKafkaContainer instance for method chaining
     */
    public StrimziKafkaContainer withLogCollection() {
        this.logFilePath = "target/strimzi-test-container-logs/";
        return self();
    }

    /**
     * Fluent method to enable log collection for all containers in the cluster with a custom log file path.
     *
     * If the path ends with "/", role-based filenames are automatically appended at runtime for each container:
     *      Controller-only: "kafka-controller-{nodeId}.log"
     *      Broker-only: "kafka-broker-{nodeId}.log"
     *      Combined: "kafka-container-{nodeId}.log"
     * otherwise,  the path doesn't end with "/", it's used as the exact filename base for all containers
     *
     * @param logFilePath the base path where container logs will be saved. Use "/" suffix for automatic role-based naming.
     * @return the current instance of {@code StrimziKafkaClusterBuilder} for method chaining
     */
    public StrimziKafkaContainer withLogCollection(final String logFilePath) {
        this.logFilePath = Utils.requireNonBlank(logFilePath, "Log file path");
        return self();
    }

    /**
     * Initialize the Proxy instance for this Kafka broker.
     *
     * This method performs lazy initialization of the proxy. If the proxy has not been
     * initialized, it creates one using the Toxiproxy client. If the Toxiproxy client is not initialized,
     * it is created using the host and control port of the proxy container.
     *
     * @throws IllegalStateException    if the proxy container has not been configured.
     * @throws RuntimeException         if an IOException occurs during the creation of the Proxy.
     */
    /* test */ synchronized void initializeProxy() {
        if (this.proxyContainer == null) {
            throw new IllegalStateException("The proxy container has not been configured");
        }

        if (this.proxy == null) {
            if (this.toxiproxyClient == null) {
                this.toxiproxyClient = new ToxiproxyClient(proxyContainer.getHost(), proxyContainer.getControlPort());
            }
            try {
                final int listenPort = TOXIPROXY_PORT_BASE + this.nodeId;
                final String upstream = NETWORK_ALIAS_PREFIX + this.nodeId + ":" + KAFKA_PORT;
                this.proxy = this.toxiproxyClient.createProxy("kafka" + this.nodeId, "0.0.0.0:" + listenPort, upstream);
            } catch (IOException e) {
                LOGGER.error("Error happened during creation of the Proxy: {}", e.getMessage());
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Retrieves the Proxy instance for this Kafka broker.
     *
     * @return Proxy instance for this Kafka broker.
     */
    public Proxy getProxy() {
        initializeProxy();
        return this.proxy;
    }

    /* test */ String getKafkaVersion() {
        return this.kafkaVersion;
    }

    /**
     * Gets the node ID for this Kafka container.
     *
     * @return the node ID
     */
    public int getNodeId() {
        return nodeId;
    }

    /* test */ Map<String, String> getKafkaConfigurationMap() {
        return kafkaConfigurationMap;
    }

    /**
     * Checks if OAuth is enabled.
     *
     * @return {@code true} if OAuth is enabled; {@code false} otherwise
     */
    public boolean isOAuthEnabled() {
        return this.oauthEnabled;
    }

    /**
     * Gets the SASL username for authentication.
     *
     * @return the SASL username
     */
    public String getSaslUsername() {
        return saslUsername;
    }

    /**
     * Gets the SASL password for authentication.
     *
     * @return the SASL password
     */
    public String getSaslPassword() {
        return saslPassword;
    }

    /**
     * Gets the OAuth realm.
     *
     * @return the OAuth realm
     */
    public String getRealm() {
        return realm;
    }

    /**
     * Gets the OAuth client ID.
     *
     * @return the OAuth client ID
     */
    public String getClientId() {
        return clientId;
    }

    /**
     * Gets the OAuth client secret.
     *
     * @return the OAuth client secret
     */
    public String getClientSecret() {
        return clientSecret;
    }

    /**
     * Gets the OAuth URI.
     *
     * @return the OAuth URI
     */
    public String getOauthUri() {
        return oauthUri;
    }

    /**
     * Gets the OAuth username claim.
     *
     * @return the OAuth username claim
     */
    public String getUsernameClaim() {
        return usernameClaim;
    }

    /**
     * Gets the authentication type configured for this container.
     *
     * @return the authentication type
     */
    public AuthenticationType getAuthenticationType() {
        return authenticationType;
    }

    /**
     * Gets the node role for this Kafka container.
     *
     * @return the node role
     */
    public KafkaNodeRole getNodeRole() {
        return nodeRole;
    }

    /* test */ String getLogFilePath() {
        return logFilePath;
    }

    /* test */ Function<StrimziKafkaContainer, String> getBootstrapServersProvider() {
        return bootstrapServersProvider;
    }

    /* test */ int getFixedExposedPort() {
        return fixedExposedPort;
    }

    /* test */ boolean hasWaitForRunningConfigured() {
        return this.getWaitStrategy() instanceof LogMessageWaitStrategy;
    }
}
