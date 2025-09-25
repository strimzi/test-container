/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import eu.rekawek.toxiproxy.Proxy;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.matchesPattern;

@SuppressWarnings({"ClassFanOutComplexity", "ClassDataAbstractionCoupling"})
public class StrimziKafkaClusterIT extends AbstractIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(StrimziKafkaClusterIT.class);
    private static final int NUMBER_OF_REPLICAS = 3;

    private StrimziKafkaCluster systemUnderTest;

    @Test
    void testKafkaClusterStartup() throws InterruptedException, ExecutionException {
        setUpKafkaKRaftCluster();

        verifyReadinessOfKRaftCluster();
    }

    @Test
    void testKafkaClusterStartupWithSharedNetwork() throws InterruptedException, ExecutionException {
        systemUnderTest = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(NUMBER_OF_REPLICAS)
            .withSharedNetwork()
            .build();
        systemUnderTest.start();

        verifyReadinessOfKRaftCluster();
    }

    @Test
    void testKafkaClusterFunctionality() throws ExecutionException, InterruptedException, TimeoutException {
        setUpKafkaKRaftCluster();

        verifyFunctionalityOfKafkaCluster();
    }

    @Test
    void testKafkaClusterWithSharedNetworkFunctionality() throws ExecutionException, InterruptedException, TimeoutException {
        systemUnderTest = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(NUMBER_OF_REPLICAS)
            .withSharedNetwork()
            .build();
        systemUnderTest.start();

        verifyFunctionalityOfKafkaCluster();
    }

    @Test
    void testStartClusterWithProxyContainer() {
        ToxiproxyContainer proxyContainer = new ToxiproxyContainer(
                DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.11.0")
                        .asCompatibleSubstituteFor("shopify/toxiproxy"));

        systemUnderTest = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(NUMBER_OF_REPLICAS)
            .withProxyContainer(proxyContainer)
            .build();

        systemUnderTest.start();
        List<String> bootstrapUrls = new ArrayList<>();
        for (KafkaContainer kafkaContainer : systemUnderTest.getBrokers()) {
            Proxy proxy = ((StrimziKafkaContainer) kafkaContainer).getProxy();
            assertThat(proxy, notNullValue());
            bootstrapUrls.add(kafkaContainer.getBootstrapServers());
        }

        assertThat(systemUnderTest.getBootstrapServers(),
            is(String.join(",", bootstrapUrls)));
    }

    @Test
    void testCanCopyJarAndExposePort() throws IOException, InterruptedException {
        final Path tempJar = Files.createTempFile("dummy-plugin", ".jar");

        try (JarOutputStream jarOut = new JarOutputStream(Files.newOutputStream(tempJar))) {
            JarEntry entry = new JarEntry("com/example/Dummy.class");
            jarOut.putNextEntry(entry);
            jarOut.write("noop".getBytes(StandardCharsets.UTF_8)); // fake bytecode
            jarOut.closeEntry();
        }

        systemUnderTest = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(1)
            .build();

        systemUnderTest.start();

        // Inject the generated JAR into the container
        final String containerPath = "/opt/kafka/plugins/dummy-plugin.jar";
        for (GenericContainer<?> container : systemUnderTest.getNodes()) {
            container.copyFileToContainer(
                MountableFile.forHostPath(tempJar),
                containerPath
            );
        }

        // Verify the file was copied
        final Container.ExecResult result = systemUnderTest.getNodes().iterator().next()
            .execInContainer("ls", "/opt/kafka/plugins/");
        assertThat(result.getStdout(), CoreMatchers.containsString("dummy-plugin.jar"));
    }

    @Test
    void testDedicatedRolesClusterStartsAndFunctionsProperly() throws InterruptedException, ExecutionException, TimeoutException {
        try (StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(5)
            .withDedicatedRoles()
            .withNumberOfControllers(3)
            .build()) {

            cluster.start();

            // Verify cluster configuration
            assertThat(cluster.isUsingDedicatedRoles(), is(true));
            assertThat(cluster.getNodes().size(), is(8)); // 3 controllers + 5 brokers
            assertThat(cluster.getControllers().size(), is(3));
            assertThat(cluster.getBrokers().size(), is(5));

            // Verify bootstrap servers are available
            String bootstrapServers = cluster.getBootstrapServers();
            assertThat(bootstrapServers, notNullValue());

            // Should have exactly 5 broker endpoints
            String[] servers = bootstrapServers.split(",");
            assertThat(servers.length, is(5));

            // Verify network bootstrap servers
            String networkBootstrapServers = cluster.getNetworkBootstrapServers();
            assertThat(networkBootstrapServers, notNullValue());
            assertThat(networkBootstrapServers.split(",").length, is(5));

            // Set systemUnderTest for the verification method
            this.systemUnderTest = cluster;
            verifyFunctionalityOfKafkaCluster();
        }
    }

    private void setUpKafkaKRaftCluster() {
        systemUnderTest = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(NUMBER_OF_REPLICAS)
            .build();
        systemUnderTest.start();
    }

    private void verifyReadinessOfKRaftCluster() throws InterruptedException, ExecutionException {
        try (Admin adminClient = AdminClient.create(ImmutableMap.of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, systemUnderTest.getBootstrapServers()))) {
            // Check broker availability
            Collection<Node> brokers = adminClient.describeCluster().nodes().get();
            assertThat(brokers, notNullValue());
            // Check if we have 3 brokers
            assertThat(brokers.size(), CoreMatchers.equalTo(3));

            // Optionally check controller status
            Node controller = adminClient.describeCluster().controller().get();
            assertThat(controller, notNullValue());

            LOGGER.info("Brokers are {}", systemUnderTest.getBootstrapServers());
        }
    }

    private void verifyFunctionalityOfKafkaCluster() throws ExecutionException, InterruptedException, TimeoutException {
        // using try-with-resources for AdminClient, KafkaProducer and KafkaConsumer (implicit closing connection)
        try (final AdminClient adminClient = AdminClient.create(ImmutableMap.of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, systemUnderTest.getBootstrapServers()));
             KafkaProducer<String, String> producer = new KafkaProducer<>(
                 ImmutableMap.of(
                     ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, systemUnderTest.getBootstrapServers(),
                     ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
                 ),
                 new StringSerializer(),
                 new StringSerializer()
             );
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                 ImmutableMap.of(
                     ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, systemUnderTest.getBootstrapServers(),
                     ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID(),
                     ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  OffsetResetStrategy.EARLIEST.name().toLowerCase(Locale.ROOT)
                 ),
                 new StringDeserializer(),
                 new StringDeserializer()
             )
        ) {
            final String topicName = "example-topic";
            final String recordKey = "strimzi";
            final String recordValue = "the-best-project-in-the-world";

            final Collection<NewTopic> topics = Collections.singletonList(new NewTopic(topicName, NUMBER_OF_REPLICAS, (short) NUMBER_OF_REPLICAS));
            adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);

            consumer.subscribe(Collections.singletonList(topicName));

            producer.send(new ProducerRecord<>(topicName, recordKey, recordValue)).get();

            Utils.waitFor("Consumer records are present", Duration.ofSeconds(10), Duration.ofMinutes(2),
                () -> {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    if (records.isEmpty()) {
                        return false;
                    }

                    // verify count
                    assertThat(records.count(), is(1));

                    ConsumerRecord<String, String> consumerRecord = records.records(topicName).iterator().next();

                    // verify content of the record
                    assertThat(consumerRecord.topic(), is(topicName));
                    assertThat(consumerRecord.key(), is(recordKey));
                    assertThat(consumerRecord.value(), is(recordValue));

                    return true;
                });
        }
    }

    @Test
    void testGetNetworkBootstrapControllersWhenUsingDedicatedRoles() {
        this.systemUnderTest = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(1)
            .withDedicatedRoles()
            .withNumberOfControllers(3)
            .build();

        this.systemUnderTest.start();

        String networkBootstrapControllers = systemUnderTest.getNetworkBootstrapControllers();
        assertThat(networkBootstrapControllers, notNullValue());
        assertThat(networkBootstrapControllers, not(""));

        String[] controllers = networkBootstrapControllers.split(",");
        assertThat(controllers.length, is(3));

        for (String controller : controllers) {
            assertThat(controller, matchesPattern("broker-[0-9]+:9094"));
        }
    }

    @Test
    void testExternalClientCanConnectDirectlyToControllersWhenUsingDedicatedRoles() throws ExecutionException, InterruptedException, TimeoutException {
        this.systemUnderTest = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(1)
            .withDedicatedRoles()
            .withNumberOfControllers(1)
            .build();

        this.systemUnderTest.start();

        String bootstrapControllers = this.systemUnderTest.getBootstrapControllers();
        LOGGER.info("Bootstrap controllers: {}", bootstrapControllers);

        try (AdminClient adminClient = AdminClient.create(ImmutableMap.of(
            AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG, bootstrapControllers,
            AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000,
            AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30000))) {

            Collection<Node> nodes = adminClient.describeCluster().nodes().get(30, TimeUnit.SECONDS);
            assertThat(nodes, notNullValue());

            Node controller = adminClient.describeCluster().controller().get(30, TimeUnit.SECONDS);
            assertThat(controller, notNullValue());
            LOGGER.info("Controller ID: {}", controller.id());

        }
    }

    @Test
    void testLogCollectionWithDedicatedRoles() throws IOException {
        // Expected log files for dedicated roles cluster
        String controller0LogPath = "target/strimzi-test-container-logs/kafka-controller-0.log";
        String controller1LogPath = "target/strimzi-test-container-logs/kafka-controller-1.log";
        String controller2LogPath = "target/strimzi-test-container-logs/kafka-controller-2.log";
        String brokerLogPath = "target/strimzi-test-container-logs/kafka-broker-3.log";

        Path controller0Path = Paths.get(controller0LogPath);
        Path controller1Path = Paths.get(controller1LogPath);
        Path controller2Path = Paths.get(controller2LogPath);
        Path brokerPath = Paths.get(brokerLogPath);

        // Clean up any existing log files
        Files.deleteIfExists(controller0Path);
        Files.deleteIfExists(controller1Path);
        Files.deleteIfExists(controller2Path);
        Files.deleteIfExists(brokerPath);

        this.systemUnderTest = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(1)
            .withDedicatedRoles()
            .withNumberOfControllers(3)
            .withLogCollection()
            .build();

        this.systemUnderTest.start();

        // Verify cluster is running
        assertThat(this.systemUnderTest.getBootstrapServers(), notNullValue());
        assertThat(this.systemUnderTest.getBootstrapControllers(), notNullValue());

        this.systemUnderTest.stop();

        // Verify log files were created for dedicated roles
        assertThat("Controller log file should exist", Files.exists(controller0Path), is(true));
        assertThat("Controller log file should exist", Files.exists(controller1Path), is(true));
        assertThat("Controller log file should exist", Files.exists(controller2Path), is(true));
        assertThat("Broker log file should exist", Files.exists(brokerPath), is(true));

        // Verify log files contain expected content
        String controller0Content = Files.readString(controller0Path);
        String controller1Content = Files.readString(controller1Path);
        String controller2Content = Files.readString(controller2Path);
        String brokerContent = Files.readString(brokerPath);

        assertThat(controller0Content, CoreMatchers.containsString("Kafka Server started"));
        assertThat(controller1Content, CoreMatchers.containsString("Kafka Server started"));
        assertThat(controller2Content, CoreMatchers.containsString("Kafka Server started"));
        assertThat(brokerContent, CoreMatchers.containsString("Kafka Server started"));

        // Clean up
        Files.deleteIfExists(controller0Path);
        Files.deleteIfExists(controller1Path);
        Files.deleteIfExists(controller2Path);
        Files.deleteIfExists(brokerPath);
    }

    @Test
    void testLogCollectionWithCombinedRoles() throws IOException {
        // Expected log files for combined roles cluster
        String combinedLogPath1 = "target/strimzi-test-container-logs/kafka-container-0.log";
        String combinedLogPath2 = "target/strimzi-test-container-logs/kafka-container-1.log";

        Path combinedPath1 = Paths.get(combinedLogPath1);
        Path combinedPath2 = Paths.get(combinedLogPath2);

        // Clean up any existing log files
        Files.deleteIfExists(combinedPath1);
        Files.deleteIfExists(combinedPath2);

        this.systemUnderTest = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(2)
            .withLogCollection()
            .build();

        this.systemUnderTest.start();

        // Verify cluster is running
        assertThat(this.systemUnderTest.getBootstrapServers(), notNullValue());

        this.systemUnderTest.stop();

        // Verify log files were created for combined roles
        assertThat("Combined role log file 1 should exist", Files.exists(combinedPath1), is(true));
        assertThat("Combined role log file 2 should exist", Files.exists(combinedPath2), is(true));

        // Verify log files contain expected content
        String logContent1 = Files.readString(combinedPath1);
        String logContent2 = Files.readString(combinedPath2);

        assertThat(logContent1, CoreMatchers.containsString("ControllerServer id=0"));
        assertThat(logContent2, CoreMatchers.containsString("ControllerServer id=1"));

        Files.deleteIfExists(combinedPath1);
        Files.deleteIfExists(combinedPath2);
    }

    @Test
    void testLogCollectionWithCustomPath() throws IOException {
        // Custom log path for cluster
        String customLogPath = "test-cluster-logs/kafka-container-0.log";
        Path logPath = Paths.get(customLogPath);

        // Clean up any existing log file
        Files.deleteIfExists(logPath);

        this.systemUnderTest = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(1)
            .withLogFilePath("test-cluster-logs/")
            .build();

        this.systemUnderTest.start();

        // Verify cluster is running
        assertThat(this.systemUnderTest.getBootstrapServers(), notNullValue());

        this.systemUnderTest.stop();

        // Verify log file was created at custom path
        assertThat("Custom cluster log file should exist", Files.exists(logPath), is(true));

        // Verify log file contains expected content
        String logContent = Files.readString(logPath);
        assertThat(logContent, CoreMatchers.containsString("ControllerServer id=0"));

        Files.deleteIfExists(logPath);
        if (logPath.getParent() != null) {
            Files.deleteIfExists(logPath.getParent());
        }
    }

    @AfterEach
    void afterEach() {
        if (this.systemUnderTest != null) {
            this.systemUnderTest.stop();
        }
    }
}
