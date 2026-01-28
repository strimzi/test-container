/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A Kafka Connect cluster using the latest image from quay.io/strimzi/kafka with the given version.
 * Kafka Connect is started in distributed mode. Users must use the exposed REST API to start, stop and manage connectors.
 */
public class StrimziConnectCluster {

    private static final String NETWORK_ALIAS_PREFIX = "connect-";
    private static final int CONNECT_PORT = 8083;
    private static final int INTER_WORKER_PORT = 8084;

    private final StrimziKafkaCluster kafkaCluster;
    private final Map<String, String> additionalConnectConfiguration;
    private final String kafkaVersion;
    private final boolean includeFileConnectors;
    private final String groupId;
    private final List<StrimziConnectContainer> workers;

    /**
     * Creates a new StrimziConnectCluster using the provided builder.
     *
     * @param builder the builder containing the configuration for this cluster
     */
    public StrimziConnectCluster(StrimziConnectClusterBuilder builder) {
        this.kafkaCluster = builder.kafkaCluster;
        this.additionalConnectConfiguration = builder.additionalConnectConfiguration;
        this.kafkaVersion = builder.kafkaVersion == null
                ? KafkaVersionService.getInstance().latestRelease().getVersion()
                : builder.kafkaVersion;
        this.includeFileConnectors = builder.includeFileConnectors;
        this.groupId = builder.groupId;

        final String imageName = KafkaVersionService.strimziTestContainerImageName(kafkaVersion);

        workers = new ArrayList<>();
        for (int i = 0; i < builder.workersNum; i++) {
            final String host = NETWORK_ALIAS_PREFIX + i;
            final Properties configs = buildConfigs(host);
            StrimziConnectContainer worker = new StrimziConnectContainer(imageName, kafkaCluster, configs)
                    .withNetwork(kafkaCluster.getNetwork())
                    .withNetworkAliases(host)
                    .withExposedPorts(CONNECT_PORT)
                    .withEnv("LOG_DIR", "/tmp")
                    .waitForRunning()
                    .waitingFor(Wait.forHttp("/health").forStatusCode(200));
            workers.add(worker);
        }
    }

    private Properties buildConfigs(String host) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaCluster.getNetworkBootstrapServers());
        properties.setProperty("group.id", groupId);
        properties.setProperty("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        properties.setProperty("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        properties.setProperty("offset.storage.topic", "connect-offsets");
        properties.setProperty("offset.storage.replication.factor", "-1");
        properties.setProperty("config.storage.topic", "connect-configs");
        properties.setProperty("config.storage.replication.factor", "-1");
        properties.setProperty("status.storage.topic", "connect-status");
        properties.setProperty("status.storage.replication.factor", "-1");
        properties.setProperty("listeners", "http://:" + CONNECT_PORT + ",http://" + host + ":" + INTER_WORKER_PORT);
        properties.putAll(additionalConnectConfiguration);
        if (includeFileConnectors) {
            final String connectFileJar = "/opt/kafka/libs/connect-file-" + kafkaVersion + ".jar";
            if (properties.containsKey("plugin.path")) {
                final String pluginPath = properties.getProperty("plugin.path");
                properties.setProperty("plugin.path", pluginPath + "," + connectFileJar);
            } else {
                properties.setProperty("plugin.path", connectFileJar);
            }
        }
        return properties;
    }

    /**
     * Get the workers of this Kafka Connect cluster.
     *
     * @return collection of GenericContainer containers
     */
    @DoNotMutate
    public Collection<GenericContainer<?>> getWorkers() {
        return new ArrayList<>(workers);
    }

    /**
     * Start the Kafka Connect cluster.
     * This starts all the workers and waits for them to all be healthy and ready to be used.
     */
    @DoNotMutate
    public void start() {
        for (StrimziConnectContainer worker : workers) {
            worker.start();
        }
    }

    /**
     * Stop the Kafka Connect cluster.
     */
    @DoNotMutate
    public void stop() {
        workers.forEach(StrimziConnectContainer::stop);
    }

    /**
     * Return the REST API endpoint of one of the available workers.
     *
     * @return the REST API endpoint
     */
    @DoNotMutate
    public String getRestEndpoint() {
        for (StrimziConnectContainer worker : workers) {
            if (worker.isRunning()) {
                return "http://" + worker.getHost() + ":" + worker.getMappedPort(CONNECT_PORT);
            }
        }
        throw new IllegalStateException("No workers are running and healthy");
    }

    /**
     * Builder class for {@code StrimziConnectCluster}.
     * <p>
     * Use this builder to create instances of {@code StrimziConnectCluster}.
     * You must at least call {@link #withKafkaCluster(StrimziKafkaCluster)}, and
     * {@link #withGroupId(String)} before calling {@link #build()}.
     * </p>
     */
    public static class StrimziConnectClusterBuilder {

        private Map<String, String> additionalConnectConfiguration = new HashMap<>();
        private boolean includeFileConnectors = true;
        private int workersNum = 1;
        private String kafkaVersion;
        private StrimziKafkaCluster kafkaCluster;
        private String groupId;

        /**
         * Set the Kafka cluster the Kafka Connect cluster will use to.
         *
         * @param kafkaCluster the {@link StrimziKafkaCluster} instance
         * @return the current instance of {@code StrimziConnectClusterBuilder} for method chaining
         */
        public StrimziConnectClusterBuilder withKafkaCluster(StrimziKafkaCluster kafkaCluster) {
            this.kafkaCluster = kafkaCluster;
            return this;
        }

        /**
         * Set the number of Kafka Connect workers in the cluster.
         * If not called, the cluster has a single worker.
         *
         * @param workersNum the number of Kafka Connect workers
         * @return the current instance of {@code StrimziConnectClusterBuilder} for method chaining
         */
        public StrimziConnectClusterBuilder withNumberOfWorkers(int workersNum) {
            this.workersNum = workersNum;
            return this;
        }

        /**
         * Add additional Kafka Connect configuration parameters.
         * These configurations are applied to all workers in the cluster.
         *
         * @param additionalConnectConfiguration a map of additional Kafka Connect configuration options
         * @return the current instance of {@code StrimziConnectClusterBuilder} for method chaining
         */
        public StrimziConnectClusterBuilder withAdditionalConnectConfiguration(Map<String, String> additionalConnectConfiguration) {
            this.additionalConnectConfiguration = additionalConnectConfiguration;
            return this;
        }

        /**
         * Specify the Kafka version to be used for the Connect workers in the cluster.
         * If not called, the latest Kafka version available from {@link KafkaVersionService} will be used.
         *
         * @param kafkaVersion the desired Kafka version for the Connect cluster
         * @return the current instance of {@code StrimziConnectClusterBuilder} for method chaining
         */
        public StrimziConnectClusterBuilder withKafkaVersion(String kafkaVersion) {
            this.kafkaVersion = kafkaVersion;
            return this;
        }

        /**
         * Disable the FileStreams connectors.
         * If not called, the FileSteams connectors are added to plugin.path.
         *
         * @return the current instance of {@code StrimziConnectClusterBuilder} for method chaining
         */
        public StrimziConnectClusterBuilder withoutFileConnectors() {
            this.includeFileConnectors = false;
            return this;
        }

        /**
         * Specify the group.id of the Connect cluster.
         *
         * @param groupId the group id
         * @return the current instance of {@code StrimziConnectClusterBuilder} for method chaining
         */
        public StrimziConnectClusterBuilder withGroupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        /**
         * Build and return a {@code StrimziConnectCluster} instance based on the provided configurations.
         *
         * @return a new instance of {@code StrimziConnectCluster}
         */
        public StrimziConnectCluster build() {
            if (kafkaCluster == null) {
                throw new IllegalArgumentException("A Kafka cluster must be specified");
            }
            if (groupId == null) {
                throw new IllegalArgumentException("The Connect cluster group.id configuration must be specified");
            }
            if (workersNum <= 0) {
                throw new IllegalArgumentException("The number of workers in the Connect cluster must be greater than 0");
            }
            if (additionalConnectConfiguration == null) {
                throw new IllegalArgumentException("The additional configuration must be specified");
            }
            return new StrimziConnectCluster(this);
        }
    }

    /* test */ String getKafkaVersion() {
        return kafkaVersion;
    }
}
