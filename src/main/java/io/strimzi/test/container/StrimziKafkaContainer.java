/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ContainerNetwork;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/**
 * StrimziKafkaContainer is a single-node instance of Kafka using the image from quay.io/strimzi/kafka with the
 * given version. There are two options for how to use it. The first one is using an embedded zookeeper which will run
 * inside Kafka container. The Another option is to use @StrimziZookeeperContainer as an external Zookeeper.
 * The additional configuration for Kafka broker can be injected via constructor. This container is a good fit for
 * integration testing but for more hardcore testing we suggest using @StrimziKafkaCluster.
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
     * Lazy image name provider
     */
    private final CompletableFuture<String> imageNameProvider;

    // instance attributes
    private int kafkaExposedPort;
    private int internalZookeeperExposedPort;
    private Map<String, String> kafkaConfigurationMap;
    private String externalZookeeperConnect;
    private int brokerId;
    private String kafkaVersion;
    private boolean useKraft;
    private Function<StrimziKafkaContainer, String> bootstrapServersProvider = c -> String.format("PLAINTEXT://%s:%s", getHost(), this.kafkaExposedPort);
    private String clusterId;
    private MountableFile serverPropertiesFile;

    // proxy attributes
    private ToxiproxyContainer proxyContainer;
    private ToxiproxyClient toxiproxyClient;
    private Proxy proxy;

    /**
     * Image name is specified lazily automatically in {@link #doStart()} method
     */
    public StrimziKafkaContainer() {
        this(new CompletableFuture<>());
    }

    /**
     * Image name is specified by {@code dockerImageName}
     *
     * @param dockerImageName specific docker image name provided by constructor parameter
     */
    public StrimziKafkaContainer(String dockerImageName) {
        this(CompletableFuture.completedFuture(dockerImageName));
    }

    /**
     * Image name is lazily set in {@link #doStart()} method
     */
    private StrimziKafkaContainer(CompletableFuture<String> imageName) {
        super(imageName);
        this.imageNameProvider = imageName;
        // we need this shared network in case we deploy StrimziKafkaCluster which consist of `StrimziKafkaContainer`
        // instances and by default each container has its own network, which results in `Unable to resolve address: zookeeper:2181`
        super.setNetwork(Network.SHARED);
        // exposing kafka port from the container
        super.setExposedPorts(Collections.singletonList(KAFKA_PORT));
        super.addEnv("LOG_DIR", "/tmp");
    }

    @Override
    protected void doStart() {
        if (this.proxyContainer != null && !this.proxyContainer.isRunning()) {
            this.proxyContainer.start();

            // Instantiate a ToxiproxyClient if it has not been previously provided via configuration settings.
            if (toxiproxyClient == null) {
                toxiproxyClient = new ToxiproxyClient(this.proxyContainer.getHost(), this.proxyContainer.getControlPort());
            }
        }

        if (!this.imageNameProvider.isDone()) {
            this.imageNameProvider.complete(KafkaVersionService.strimziTestContainerImageName(this.kafkaVersion));
        }
        try {
            if (this.useKraft && ((this.kafkaVersion != null && this.kafkaVersion.startsWith("2.")) || this.imageNameProvider.get().contains("2.8.2"))) {
                throw new UnsupportedKraftKafkaVersionException("Specified Kafka version " + this.kafkaVersion + " is not supported in KRaft mode.");
            }
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("Error occurred during retrieving of image name provider", e);
            throw new RuntimeException(e);
        }
        // exposing Kafka and port from the container
        if (!this.hasKraftOrExternalZooKeeperConfigured()) {
            // expose internal ZooKeeper internal port iff external ZooKeeper or KRaft is not specified/enabled
            super.addExposedPort(StrimziZookeeperContainer.ZOOKEEPER_PORT);
        }
        // we need it for the startZookeeper(); and startKafka(); to run container before...
        super.setCommand("sh", "-c", runStarterScript());
        super.doStart();
    }

    @Override
    public void stop() {
        if (proxyContainer != null && proxyContainer.isRunning()) {
            proxyContainer.stop();
        }
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
     * @return StrimziKafkaContainer instance
     */
    public StrimziKafkaContainer waitForRunning() {
        if (this.useKraft) {
            super.waitingFor(Wait.forLogMessage(".*Transitioning from RECOVERY to RUNNING.*", 1));
        } else {
            super.waitingFor(Wait.forLogMessage(".*Recorded new.*controller, from now on will use [node|broker].*", 1));
        }
        return this;
    }

    @Override
    @SuppressWarnings({"JavaNCSS", "NPathComplexity"})
    protected void containerIsStarting(final InspectContainerResponse containerInfo, final boolean reused) {
        super.containerIsStarting(containerInfo, reused);

        this.kafkaExposedPort = getMappedPort(KAFKA_PORT);

        // retrieve internal ZooKeeper internal port iff external ZooKeeper or KRaft is not specified/enabled
        if (!this.hasKraftOrExternalZooKeeperConfigured()) {
            this.internalZookeeperExposedPort = getMappedPort(StrimziZookeeperContainer.ZOOKEEPER_PORT);
        }

        LOGGER.info("Mapped port: {}", kafkaExposedPort);

        final String bootstrapServers = getBootstrapServers();
        final String bsListenerName = extractListenerName(bootstrapServers);

        StringBuilder advertisedListeners = new StringBuilder(bootstrapServers);

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

        kafkaListeners.append(bsListenerName).append("://0.0.0.0:").append(KAFKA_PORT);
        kafkaListenerSecurityProtocol.append("PLAINTEXT:PLAINTEXT");
        if (!bsListenerName.equals("PLAINTEXT")) {
            kafkaListenerSecurityProtocol.append(",").append(bsListenerName).append(":").append(bsListenerName);
        }

        if (this.useKraft) {
            // adding Controller listener for Kraft mode
            kafkaListeners.append(",").append("CONTROLLER://localhost:9094");
            kafkaListenerSecurityProtocol.append(",").append("CONTROLLER:PLAINTEXT");
        }

        Map<String, String> kafkaConfiguration = new HashMap<>();

        kafkaConfiguration.put("listeners", kafkaListeners.toString());
        kafkaConfiguration.put("advertised.listeners", advertisedListeners.toString());
        kafkaConfiguration.put("listener.security.protocol.map", kafkaListenerSecurityProtocol.toString());
        kafkaConfiguration.put("inter.broker.listener.name", "BROKER1");
        kafkaConfiguration.put("broker.id", String.valueOf(this.brokerId));

        if (this.useKraft) {
            // explicitly say, which listener will be controller (in this case CONTROLLER)
            kafkaConfiguration.put("controller.quorum.voters", this.brokerId + "@localhost:9094");
            kafkaConfiguration.put("controller.listener.names", "CONTROLLER");
        } else if (this.externalZookeeperConnect != null) {
            LOGGER.info("Using external ZooKeeper 'zookeeper.connect={}'.", this.externalZookeeperConnect);
            kafkaConfiguration.put("zookeeper.connect", this.externalZookeeperConnect);
        } else {
            // using internal ZooKeeper
            LOGGER.info("Using internal ZooKeeper 'zookeeper.connect={}.'", "localhost:" + StrimziZookeeperContainer.ZOOKEEPER_PORT);
            kafkaConfiguration.put("zookeeper.connect", "localhost:" + StrimziZookeeperContainer.ZOOKEEPER_PORT);
        }

        // additional kafka config
        if (this.kafkaConfigurationMap != null) {
            kafkaConfiguration.putAll(this.kafkaConfigurationMap);
        }
        String kafkaConfigurationOverride = writeOverrideString(kafkaConfiguration);

        String command = "#!/bin/bash \n";

        if (!this.useKraft) {
            if (this.externalZookeeperConnect != null) {
                withEnv("KAFKA_ZOOKEEPER_CONNECT", this.externalZookeeperConnect);
            } else {
                command += "bin/zookeeper-server-start.sh config/zookeeper.properties &\n";
            }
            command += "bin/kafka-server-start.sh config/server.properties" + kafkaConfigurationOverride;
        } else {
            clusterId = this.randomUuid();
            command += "bin/kafka-storage.sh format -t=\"" + clusterId + "\" -c config/kraft/server.properties \n";
            command += "bin/kafka-server-start.sh config/kraft/server.properties" + kafkaConfigurationOverride;
        }

        Utils.asTransferableBytes(serverPropertiesFile).ifPresent(properties -> copyFileToContainer(
                properties,
                this.useKraft ? "/opt/kafka/config/kraft/server.properties" : "/opt/kafka/config/server.properties"
        ));

        LOGGER.info("Copying command to 'STARTER_SCRIPT' script.");

        copyFileToContainer(
                Transferable.of(command.getBytes(StandardCharsets.UTF_8), 700),
                STARTER_SCRIPT
        );
    }

    @Override
    public boolean hasKraftOrExternalZooKeeperConfigured() {
        return this.useKraft || this.externalZookeeperConnect != null;
    }

    private String extractListenerName(String bootstrapServers) {
        // extract listener name from given bootstrap servers
        String[] strings = bootstrapServers.split(":");
        if (strings.length < 3) {
            throw new IllegalArgumentException("The configured boostrap servers '" + bootstrapServers +
                    "' must be prefixed with a listener name.");
        }
        return strings[0];
    }

    static String writeOverrideString(Map<String, String> kafkaConfigurationMap) {
        StringBuilder kafkaConfiguration = new StringBuilder();
        kafkaConfigurationMap.forEach((configName, configValue) ->
                kafkaConfiguration
                        .append(" --override ")
                        .append('\'').append(configName.replace("'", "'\"'\"'"))
                        .append("=")
                        .append(configValue.replace("'", "'\"'\"'")).append('\''));
        return kafkaConfiguration.toString();
    }

    /**
     * In order to avoid any compile dependency on kafka-clients' <code>Uuid</code> specific class,
     * we implement our own uuid generator by replicating the Kafka's base64 encoded uuid generation logic.
     */
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

    @Override
    public String getInternalZooKeeperConnect() {
        if (this.hasKraftOrExternalZooKeeperConfigured()) {
            throw new IllegalStateException("Connect string is not available when using KRaft or external ZooKeeper");
        }
        return getHost() + ":" + this.internalZookeeperExposedPort;
    }

    @Override
    public String getBootstrapServers() {
        if (proxyContainer != null) {
            // returning the proxy host and port for indirect connection
            return String.format("PLAINTEXT://%s", getProxy().getListen());
        }
        return bootstrapServersProvider.apply(this);
    }

    /**
     * Get the cluster id. This is only supported for KRaft containers.
     * @return The cluster id.
     */
    public String getClusterId() {
        return clusterId;
    }

    /**
     * Fluent method, which sets @code{kafkaConfigurationMap}.
     *
     * @param kafkaConfigurationMap kafka configuration
     * @return StrimziKafkaContainer instance
     */
    public StrimziKafkaContainer withKafkaConfigurationMap(final Map<String, String> kafkaConfigurationMap) {
        this.kafkaConfigurationMap = kafkaConfigurationMap;
        return this;
    }

    /**
     * Fluent method, which sets @code{externalZookeeperConnect}.
     * <p>
     * If the broker was created using Kraft, this method throws an {@link IllegalArgumentException}.
     *
     * @param externalZookeeperConnect connect string
     * @return StrimziKafkaContainer instance
     */
    public StrimziKafkaContainer withExternalZookeeperConnect(final String externalZookeeperConnect) {
        if (this.useKraft) {
            throw new IllegalStateException("Cannot configure an external Zookeeper and use Kraft at the same time");
        }
        this.externalZookeeperConnect = externalZookeeperConnect;
        return self();
    }

    /**
     * Fluent method, which sets @code{brokerId}.
     *
     * @param brokerId broker id
     * @return StrimziKafkaContainer instance
     */
    public StrimziKafkaContainer withBrokerId(final int brokerId) {
        this.brokerId = brokerId;
        return self();
    }

    /**
     * Fluent method, which sets @code{kafkaVersion}.
     *
     * @param kafkaVersion kafka version
     * @return StrimziKafkaContainer instance
     */
    public StrimziKafkaContainer withKafkaVersion(final String kafkaVersion) {
        this.kafkaVersion = kafkaVersion;
        return self();
    }

    /**
     * Fluent method, which sets @code{useKraft}.
     * <p>
     * Flag to signal if we deploy Kafka with ZooKeeper or not.
     *
     * @return StrimziKafkaContainer instance
     */
    public StrimziKafkaContainer withKraft() {
        this.useKraft = true;
        return self();
    }

    /**
     * Fluent method, which sets fixed exposed port.
     *
     * @param fixedPort fixed port to expose
     * @return StrimziKafkaContainer instance
     */
    public StrimziKafkaContainer withPort(final int fixedPort) {
        if (fixedPort <= 0) {
            throw new IllegalArgumentException("The fixed Kafka port must be greater than 0");
        }
        addFixedExposedPort(fixedPort, KAFKA_PORT);
        return self();
    }

    /**
     * Fluent method, copy server properties file to the container
     *
     * @param serverPropertiesFile the mountable config file
     * @return StrimziKafkaContainer instance
     */
    public StrimziKafkaContainer withServerProperties(final MountableFile serverPropertiesFile) {
        /*
         * Save a reference to the file and delay copying to the container until the container
         * is starting. This allows for `useKraft` to be set either before or after this method
         * is called.
         */
        this.serverPropertiesFile = serverPropertiesFile;
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
            proxyContainer.setNetwork(Network.SHARED);
            proxyContainer.setNetworkAliases(Collections.singletonList("toxiproxy"));
        }
        return self();
    }

    /**
     * Retrieves a synchronized Proxy instance for this Kafka broker.
     *
     * This method ensures that only one instance of Proxy is created per broker. If the proxy has not been
     * initialized, it attempts to create one using the Toxiproxy client. If the Toxiproxy client is not initialized,
     * it is created using the host and control port of the proxy container.
     *
     * @return                          Proxy instance for this Kafka broker.
     * @throws IllegalStateException    if the proxy container has not been configured.
     * @throws RuntimeException         if an IOException occurs during the creation of the Proxy.
     */
    public synchronized Proxy getProxy() {
        if (this.proxyContainer == null) {
            throw new IllegalStateException("The proxy container has not been configured");
        }

        if (this.proxy == null) {
            if (this.toxiproxyClient == null) {
                this.toxiproxyClient = new ToxiproxyClient(proxyContainer.getHost(), proxyContainer.getControlPort());
            }
            try {
                final int listenPort = 8666 + this.brokerId;
                this.proxy = this.toxiproxyClient.createProxy("kafka" + this.brokerId, "0.0.0.0:" + listenPort, "toxiproxy:" + Utils.getFreePort());
            } catch (IOException e) {
                LOGGER.error("Error happened during creation of the Proxy: {}", e.getMessage());
                throw new RuntimeException(e);
            }
        }
        return this.proxy;
    }
}
