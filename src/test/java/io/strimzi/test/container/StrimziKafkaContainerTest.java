/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.apache.logging.log4j.Level;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.MountableFile;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StrimziKafkaContainerTest {

    private StrimziKafkaContainer kafkaContainer;

    @BeforeEach
    void setUp() {
        kafkaContainer = new StrimziKafkaContainer();
    }

    @Test
    void testDefaultInitialization() {
        assertThat(kafkaContainer, is(notNullValue()));
        assertThat(kafkaContainer.getExposedPorts(), hasItem(StrimziKafkaContainer.KAFKA_PORT));
    }

    @Test
    void testOAuthConfiguration() {
        kafkaContainer = new StrimziKafkaContainer()
            .withOAuthConfig("test-realm", "test-client", "test-secret", "http://oauth-uri", "preferred_username");

        assertThat(kafkaContainer.isOAuthEnabled(), is(true));
        assertThat(kafkaContainer.getRealm(), is("test-realm"));
        assertThat(kafkaContainer.getClientId(), is("test-client"));
        assertThat(kafkaContainer.getClientSecret(), is("test-secret"));
        assertThat(kafkaContainer.getOauthUri(), is("http://oauth-uri"));
        assertThat(kafkaContainer.getUsernameClaim(), is("preferred_username"));
    }

    @Test
    void testSettingKafkaVersion() {
        kafkaContainer = new StrimziKafkaContainer()
            .withKafkaVersion("3.3.0");

        assertThat(kafkaContainer.getKafkaVersion(), is("3.3.0"));
    }

    @Test
    void testOAuthOverPlainConfiguration() {
        kafkaContainer = new StrimziKafkaContainer()
            .withAuthenticationType(AuthenticationType.OAUTH_OVER_PLAIN)
            .withSaslUsername("test-user")
            .withSaslPassword("test-password");

        assertThat(kafkaContainer.getSaslUsername(), is("test-user"));
        assertThat(kafkaContainer.getSaslPassword(), is("test-password"));
    }

    @Test
    void testBootstrapServersWithoutProxy() {
        StrimziKafkaContainer kafkaContainer = new StrimziKafkaContainer();
        assertThat(kafkaContainer.getBootstrapServers(), startsWith("PLAINTEXT://"));
    }

    @Test
    void testBootstrapServersWithProxy() {
        ToxiproxyContainer proxyContainer = new ToxiproxyContainer();
        StrimziKafkaContainer kafkaContainer = new StrimziKafkaContainer()
            .withProxyContainer(proxyContainer);

        assertThrows(IllegalStateException.class, kafkaContainer::getProxy);
    }

    @Test
    void testUnsupportedVersionThrowsException() {
        StrimziKafkaContainer kafkaContainer = new StrimziKafkaContainer()
            .withKafkaVersion("2.8.2");

        // "Specified Kafka version 2.8.2 is not supported ."
        assertThrows(UnknownKafkaVersionException.class, kafkaContainer::doStart);
    }

    @Test
    void testGetProxyWithoutConfigurationThrowsException() {
        assertThrows(IllegalStateException.class, kafkaContainer::getProxy);
    }

    @Test
    void testWithPortThrowsExceptionOnInvalidPort() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
            () -> kafkaContainer.withPort(-1));
        assertThat(exception.getMessage(), is("The fixed Kafka port must be greater than 0"));
    }

    @Test
    void testExtractListenerNameThrowsExceptionOnInvalidBootstrap() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
            () -> kafkaContainer.extractListenerName("PLAINTEXT://localhost"));
        assertThat(exception.getMessage(), containsString("must be prefixed with a listener name"));
    }

    @Test
    void testRunStarterScriptReturnsCorrectScript() {
        String script = kafkaContainer.runStarterScript();
        assertThat(script, is("while [ ! -x /testcontainers_start.sh ]; do sleep 0.1; done; /testcontainers_start.sh"));
    }

    @Test
    void testSetAuthenticationType() {
        kafkaContainer.withAuthenticationType(AuthenticationType.OAUTH_BEARER);
        assertThat(kafkaContainer.getAuthenticationType(), is(AuthenticationType.OAUTH_BEARER));
    }

    @Test
    void testWithSaslUsernameThrowsExceptionOnInvalidInput() {
        assertThrows(IllegalArgumentException.class, () -> kafkaContainer.withSaslUsername(""));
    }

    @Test
    void testWithSaslPasswordThrowsExceptionOnInvalidInput() {
        assertThrows(IllegalArgumentException.class, () -> kafkaContainer.withSaslPassword(""));
    }

    @Test
    void testContainerNetworkIsShared() {
        assertThat(kafkaContainer.getNetwork(), is(Network.SHARED));
    }

    @Test
    void testContainerHasLogDirEnvVariable() {
        Map<String, String> envVars = kafkaContainer.getEnvMap();
        assertThat(envVars.get("LOG_DIR"), is("/tmp"));
    }

    @Test
    void testWithAuthenticationTypeNullDoesNotChangeType() {
        kafkaContainer.withAuthenticationType(AuthenticationType.OAUTH_BEARER);
        kafkaContainer.withAuthenticationType(null);

        assertThat(kafkaContainer.getAuthenticationType(), is(AuthenticationType.OAUTH_BEARER));
    }

    @Test
    void testWithNodeIdReturnsSelf() {
        StrimziKafkaContainer result = kafkaContainer.withNodeId(1);
        assertSame(kafkaContainer, result, "withNodeId() should return the same instance for method chaining.");
    }

    @Test
    void testExtractListenerNameWithValidBootstrapServers() {
        kafkaContainer.withBootstrapServers(c -> "PLAINTEXT://localhost:9092");
        String listenerName = kafkaContainer.extractListenerName(kafkaContainer.getBootstrapServers());
        assertThat("Listener name should be extracted correctly.", listenerName, CoreMatchers.is("PLAINTEXT"));
    }

    @Test
    void testExtractListenerNameThrowsExceptionOnInvalidBootstrapServers() {
        String invalidBootstrap = "localhost:9092";
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> kafkaContainer.extractListenerName(invalidBootstrap),
            "Expected extractListenerName() to throw an exception for invalid bootstrap servers."
        );
        assertThat(exception.getMessage(), containsString("must be prefixed with a listener name"));
    }

    @Test
    void testWithPortAcceptsValidPort() {
        kafkaContainer.withPort(1);
        // If no exception is thrown, the test passes
    }

    @Test
    void testWithPortThrowsExceptionWhenPortIsZero() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> kafkaContainer.withPort(0),
            "Expected withPort() to throw an exception when port is zero."
        );
        assertThat(exception.getMessage(), containsString("must be greater than 0"));
    }

    @Test
    void testWithPortThrowsExceptionWhenPortIsNegative() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> kafkaContainer.withPort(-1),
            "Expected withPort() to throw an exception when port is negative."
        );
        assertThat(exception.getMessage(), containsString("must be greater than 0"));
    }

    @Test
    void testIsOAuthEnabledReturnsFalseByDefault() {
        kafkaContainer = new StrimziKafkaContainer();
        assertThat("Expected isOAuthEnabled() to return false by default.",
            kafkaContainer.isOAuthEnabled(), CoreMatchers.is(false));
    }

    @Test
    void testOverridePropertiesWithNullOverrides() {
        Properties defaultProperties = new Properties();
        defaultProperties.setProperty("key1", "value1");
        defaultProperties.setProperty("key2", "value2");

        String result = kafkaContainer.overrideProperties(defaultProperties, null);

        assertThat("Result should contain key1=value1", result, containsString("key1=value1"));
        assertThat("Result should contain key2=value2", result, containsString("key2=value2"));
    }

    @Test
    void testOverridePropertiesWithEmptyOverrides() {
        Properties defaultProperties = new Properties();
        defaultProperties.setProperty("key1", "value1");
        defaultProperties.setProperty("key2", "value2");

        String result = kafkaContainer.overrideProperties(defaultProperties, Collections.emptyMap());

        assertThat("Result should contain key1=value1", result, containsString("key1=value1"));
        assertThat("Result should contain key2=value2", result, containsString("key2=value2"));
    }

    @Test
    void testOverridePropertiesWithOverrides() {
        Properties defaultProperties = new Properties();
        defaultProperties.setProperty("key1", "value1");
        defaultProperties.setProperty("key2", "value2");

        Map<String, String> overrides = Map.of("key2", "newValue2", "key3", "value3");

        String result = kafkaContainer.overrideProperties(defaultProperties, overrides);

        assertThat("Result should contain key1=value1", result, containsString("key1=value1"));
        assertThat("Result should contain key2=newValue2", result, containsString("key2=newValue2"));
        assertThat("Result should contain key3=value3", result, containsString("key3=value3"));
    }

    @Test
    void testConfigureListenerSecurityProtocolMap() {
        kafkaContainer.listenerNames.add("PLAINTEXT");
        kafkaContainer.listenerNames.add("SSL");
        String result = kafkaContainer.configureListenerSecurityProtocolMap("SASL_PLAINTEXT");

        assertThat(result, is("PLAINTEXT:SASL_PLAINTEXT,SSL:SASL_PLAINTEXT"));
    }

    @Test
    void testConfigureOAuthOverPlain() {
        Properties properties = new Properties();

        kafkaContainer.listenerNames.add("PLAINTEXT");
        kafkaContainer.withSaslUsername("test-user");
        kafkaContainer.withSaslPassword("test-password");

        kafkaContainer.configureOAuthOverPlain(properties);

        assertThat(properties.getProperty("sasl.enabled.mechanisms"), is("PLAIN"));
        assertThat(properties.getProperty("sasl.mechanism.inter.broker.protocol"), is("PLAIN"));
        assertThat(properties.getProperty("listener.security.protocol.map"), is("PLAINTEXT:SASL_PLAINTEXT"));
        assertThat(properties.getProperty("sasl.mechanism.controller.protocol"), is("PLAIN"));
        assertThat(properties.getProperty("principal.builder.class"), is("io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder"));

        String expectedJaasConfig = String.format(
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
            kafkaContainer.getSaslUsername(),
            kafkaContainer.getSaslPassword()
        );

        String listenerNameLowerCase = "plaintext";

        assertThat(properties.getProperty("listener.name." + listenerNameLowerCase + ".plain.sasl.jaas.config"), is(expectedJaasConfig));
        assertThat(properties.getProperty("listener.name." + listenerNameLowerCase + ".plain.sasl.server.callback.handler.class"), is("io.strimzi.kafka.oauth.server.plain.JaasServerOauthOverPlainValidatorCallbackHandler"));
    }

    @Test
    void testConfigureOAuthBearer() {
        Properties properties = new Properties();

        kafkaContainer.listenerNames.add("PLAINTEXT");
        kafkaContainer.withOAuthConfig("test-realm", "test-client-id", "test-client-secret", "http://oauth-server", "preferred_username");

        kafkaContainer.configureOAuthBearer(properties);

        assertThat(properties.getProperty("sasl.enabled.mechanisms"), is("OAUTHBEARER"));
        assertThat(properties.getProperty("sasl.mechanism.inter.broker.protocol"), is("OAUTHBEARER"));
        assertThat(properties.getProperty("listener.security.protocol.map"), is("PLAINTEXT:SASL_PLAINTEXT"));
        assertThat(properties.getProperty("sasl.mechanism.controller.protocol"), is("OAUTHBEARER"));
        assertThat(properties.getProperty("principal.builder.class"), is("io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder"));

        String expectedJaasConfig = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required ;";

        String listenerNameLowerCase = "plaintext";

        assertThat(properties.getProperty("listener.name." + listenerNameLowerCase + ".oauthbearer.sasl.jaas.config"), is(expectedJaasConfig));
        assertThat(properties.getProperty("listener.name." + listenerNameLowerCase + ".oauthbearer.sasl.server.callback.handler.class"), is("io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler"));
        assertThat(properties.getProperty("listener.name." + listenerNameLowerCase + ".oauthbearer.sasl.login.callback.handler.class"), is("io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler"));
    }

    @Test
    void testBuildDefaultServerPropertiesWithKRaft() {
        String listeners = "PLAINTEXT://0.0.0.0:9092";
        String advertisedListeners = "PLAINTEXT://localhost:9092";
        kafkaContainer.listenerNames.add("PLAINTEXT");
        kafkaContainer
            .withBrokerId(1)
            .withNodeId(1)
            .withAuthenticationType(AuthenticationType.NONE);

        Properties properties = kafkaContainer.buildDefaultServerProperties(listeners, advertisedListeners);

        // Common properties
        assertThat(properties.getProperty("listeners"), is(listeners));
        assertThat(properties.getProperty("advertised.listeners"), is(advertisedListeners));
        assertThat(properties.getProperty("inter.broker.listener.name"), is("BROKER1"));
        assertThat(properties.getProperty("broker.id"), is("1"));
        assertThat(properties.getProperty("listener.security.protocol.map"), is("PLAINTEXT:PLAINTEXT"));
        assertThat(properties.getProperty("num.network.threads"), is("3"));
        assertThat(properties.getProperty("num.io.threads"), is("8"));
        assertThat(properties.getProperty("socket.send.buffer.bytes"), is("102400"));
        assertThat(properties.getProperty("socket.receive.buffer.bytes"), is("102400"));
        assertThat(properties.getProperty("socket.request.max.bytes"), is("104857600"));
        assertThat(properties.getProperty("log.dirs"), is("/tmp/default-log-dir"));
        assertThat(properties.getProperty("num.partitions"), is("1"));
        assertThat(properties.getProperty("num.recovery.threads.per.data.dir"), is("1"));
        assertThat(properties.getProperty("offsets.topic.replication.factor"), is("1"));
        assertThat(properties.getProperty("transaction.state.log.replication.factor"), is("1"));
        assertThat(properties.getProperty("transaction.state.log.min.isr"), is("1"));
        assertThat(properties.getProperty("log.retention.hours"), is("168"));
        assertThat(properties.getProperty("log.retention.check.interval.ms"), is("300000"));

        // KRaft-specific properties
        assertThat(properties.getProperty("process.roles"), is("broker,controller"));
        assertThat(properties.getProperty("node.id"), is("1"));
        assertThat(properties.getProperty("controller.quorum.voters"), is("1@broker-1:9094"));
        assertThat(properties.getProperty("controller.listener.names"), is("CONTROLLER"));

        // zookeeper.connect should not be set
        assertThat("zookeeper.connect should not be set when useKraft is true", properties.getProperty("zookeeper.connect"), nullValue());
    }

    @Test
    void testBuildDefaultServerPropertiesWithAuthenticationTypeButOAuthNotEnabled() {
        String listeners = "PLAINTEXT://0.0.0.0:9092";
        String advertisedListeners = "PLAINTEXT://localhost:9092";
        kafkaContainer.listenerNames.add("PLAINTEXT");
        kafkaContainer
            .withBrokerId(1)
            .withNodeId(1)
            .withAuthenticationType(AuthenticationType.OAUTH_BEARER);
            // OAuth is not enabled, no OAuth config is provided

        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> kafkaContainer.buildDefaultServerProperties(listeners, advertisedListeners),
            "Expected buildDefaultServerProperties() to throw an exception when OAuth is not enabled but authentication type is set."
        );

        assertThat(exception.getMessage(), containsString("OAuth2 is not enabled"));
    }

    @Test
    void testBuildDefaultServerPropertiesWithOAutPlain() {
        String listeners = "SASL_PLAINTEXT://0.0.0.0:9092";
        String advertisedListeners = "SASL_PLAINTEXT://localhost:9092";
        kafkaContainer.listenerNames.add("SASL_PLAINTEXT");
        kafkaContainer
            .withBrokerId(1)
            .withNodeId(1)
            .withSaslUsername("admin")
            .withSaslPassword("password")
            .withOAuthConfig("test-realm", "test-client", "test-secret", "http://oauth-uri", "preferred_username")
            .withAuthenticationType(AuthenticationType.OAUTH_OVER_PLAIN);

        Properties properties = kafkaContainer.buildDefaultServerProperties(listeners, advertisedListeners);

        assertThat(properties, notNullValue());
        assertThat(properties.getProperty("listeners"), is(listeners));
        assertThat(properties.getProperty("advertised.listeners"), is(advertisedListeners));
        assertThat(properties.getProperty("inter.broker.listener.name"), is("BROKER1"));
        assertThat(properties.getProperty("broker.id"), is("1"));
        assertThat(properties.getProperty("listener.security.protocol.map"), is(kafkaContainer.configureListenerSecurityProtocolMap("SASL_PLAINTEXT")));
        assertThat(properties.getProperty("num.network.threads"), is("3"));
        assertThat(properties.getProperty("num.io.threads"), is("8"));
        assertThat(properties.getProperty("socket.send.buffer.bytes"), is("102400"));
        assertThat(properties.getProperty("socket.receive.buffer.bytes"), is("102400"));
        assertThat(properties.getProperty("socket.request.max.bytes"), is("104857600"));
        assertThat(properties.getProperty("log.dirs"), is("/tmp/default-log-dir"));
        assertThat(properties.getProperty("num.partitions"), is("1"));
        assertThat(properties.getProperty("num.recovery.threads.per.data.dir"), is("1"));
        assertThat(properties.getProperty("offsets.topic.replication.factor"), is("1"));
        assertThat(properties.getProperty("transaction.state.log.replication.factor"), is("1"));
        assertThat(properties.getProperty("transaction.state.log.min.isr"), is("1"));
        assertThat(properties.getProperty("log.retention.hours"), is("168"));
        assertThat(properties.getProperty("log.retention.check.interval.ms"), is("300000"));

        // KRaft-specific assertions
        assertThat(properties.getProperty("process.roles"), is("broker,controller"));
        assertThat(properties.getProperty("node.id"), is("1"));
        String expectedQuorumVoters = String.format("%d@%s%d:9094", 1, "broker-", 1);
        assertThat(properties.getProperty("controller.quorum.voters"), is(expectedQuorumVoters));
        assertThat(properties.getProperty("controller.listener.names"), is("CONTROLLER"));

        // OAuth-specific assertions
        assertThat(properties.getProperty("sasl.enabled.mechanisms"), is("PLAIN"));
        assertThat(properties.getProperty("sasl.mechanism.inter.broker.protocol"), is("PLAIN"));

        assertThat(properties.getProperty("listener.name.sasl_plaintext.plain.sasl.jaas.config"),
            is("org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"admin\" " +
                "password=\"password\";"));
        assertThat(properties.getProperty("listener.name.sasl_plaintext.plain.sasl.server.callback.handler.class"),
            is("io.strimzi.kafka.oauth.server.plain.JaasServerOauthOverPlainValidatorCallbackHandler"));
    }

    @Test
    void testBuildDefaultServerPropertiesWithOAuthBearer() {
        String listeners = "SASL_PLAINTEXT://0.0.0.0:9092";
        String advertisedListeners = "SASL_PLAINTEXT://localhost:9092";
        kafkaContainer.listenerNames.add("SASL_PLAINTEXT");
        kafkaContainer
            .withBrokerId(1)
            .withNodeId(1)
            .withOAuthConfig("test-realm", "test-client", "test-secret", "http://oauth-uri", "preferred_username")
            .withAuthenticationType(AuthenticationType.OAUTH_BEARER);

        Properties properties = kafkaContainer.buildDefaultServerProperties(listeners, advertisedListeners);

        assertThat(properties, notNullValue());
        assertThat(properties.getProperty("listeners"), is(listeners));
        assertThat(properties.getProperty("advertised.listeners"), is(advertisedListeners));
        assertThat(properties.getProperty("inter.broker.listener.name"), is("BROKER1"));
        assertThat(properties.getProperty("broker.id"), is("1"));
        assertThat(properties.getProperty("listener.security.protocol.map"), is(kafkaContainer.configureListenerSecurityProtocolMap("SASL_PLAINTEXT")));
        assertThat(properties.getProperty("num.network.threads"), is("3"));
        assertThat(properties.getProperty("num.io.threads"), is("8"));
        assertThat(properties.getProperty("socket.send.buffer.bytes"), is("102400"));
        assertThat(properties.getProperty("socket.receive.buffer.bytes"), is("102400"));
        assertThat(properties.getProperty("socket.request.max.bytes"), is("104857600"));
        assertThat(properties.getProperty("log.dirs"), is("/tmp/default-log-dir"));
        assertThat(properties.getProperty("num.partitions"), is("1"));
        assertThat(properties.getProperty("num.recovery.threads.per.data.dir"), is("1"));
        assertThat(properties.getProperty("offsets.topic.replication.factor"), is("1"));
        assertThat(properties.getProperty("transaction.state.log.replication.factor"), is("1"));
        assertThat(properties.getProperty("transaction.state.log.min.isr"), is("1"));
        assertThat(properties.getProperty("log.retention.hours"), is("168"));
        assertThat(properties.getProperty("log.retention.check.interval.ms"), is("300000"));

        // KRaft-specific assertions
        assertThat(properties.getProperty("process.roles"), is("broker,controller"));
        assertThat(properties.getProperty("node.id"), is("1"));
        String expectedQuorumVoters = String.format("%d@%s%d:9094", 1, "broker-", 1);
        assertThat(properties.getProperty("controller.quorum.voters"), is(expectedQuorumVoters));
        assertThat(properties.getProperty("controller.listener.names"), is("CONTROLLER"));

        // OAuth-specific assertions
        assertThat(properties.getProperty("sasl.enabled.mechanisms"), is("OAUTHBEARER"));
        assertThat(properties.getProperty("sasl.mechanism.inter.broker.protocol"), is("OAUTHBEARER"));

        assertThat(properties.getProperty("listener.name.sasl_plaintext.oauthbearer.sasl.jaas.config"),
            is("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required ;"));
        assertThat(properties.getProperty("listener.name.sasl_plaintext.oauthbearer.sasl.login.callback.handler.class"),
            is("io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler"));
        assertThat(properties.getProperty("listener.name.sasl_plaintext.oauthbearer.sasl.server.callback.handler.class"),
            is("io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler"));
    }

    @Test
    void testRandomUuidGeneratesValidUuid() throws Exception {
        // Use reflection to access the private randomUuid method
        Method randomUuidMethod = StrimziKafkaContainer.class.getDeclaredMethod("randomUuid");
        randomUuidMethod.setAccessible(true);

        String base64Uuid = (String) randomUuidMethod.invoke(kafkaContainer);

        // Decode the Base64 string
        byte[] uuidBytes = Base64.getUrlDecoder().decode(base64Uuid);

        // Convert bytes back to UUID
        ByteBuffer byteBuffer = ByteBuffer.wrap(uuidBytes);
        long mostSignificantBits = byteBuffer.getLong();
        long leastSignificantBits = byteBuffer.getLong();
        UUID uuid = new UUID(mostSignificantBits, leastSignificantBits);

        // Ensure the UUID is not equal to the skipped UUIDs
        UUID metadataTopicIdInternal = new UUID(0L, 1L);
        UUID zeroIdImpactInternal = new UUID(0L, 0L);
        assertThat(uuid, CoreMatchers.not(CoreMatchers.equalTo(metadataTopicIdInternal)));
        assertThat(uuid, CoreMatchers.not(CoreMatchers.equalTo(zeroIdImpactInternal)));

        // Additional assertions
        assertThat(uuid.version(), is(4)); // Randomly generated UUID
    }

    @Test
    void testRandomUuidDoesNotGenerateSkippedUuids() throws Exception {
        // Use reflection to access the private randomUuid method
        Method randomUuidMethod = StrimziKafkaContainer.class.getDeclaredMethod("randomUuid");
        randomUuidMethod.setAccessible(true);

        UUID metadataTopicIdInternal = new UUID(0L, 1L);
        UUID zeroIdImpactInternal = new UUID(0L, 0L);

        // Generate UUIDs and check that skipped UUIDs are not generated
        for (int i = 0; i < 1000; i++) {
            String base64Uuid = (String) randomUuidMethod.invoke(kafkaContainer);

            // Decode and reconstruct the UUID
            byte[] uuidBytes = Base64.getUrlDecoder().decode(base64Uuid);
            ByteBuffer byteBuffer = ByteBuffer.wrap(uuidBytes);
            long mostSignificantBits = byteBuffer.getLong();
            long leastSignificantBits = byteBuffer.getLong();
            UUID uuid = new UUID(mostSignificantBits, leastSignificantBits);

            assertThat(uuid, CoreMatchers.not(CoreMatchers.equalTo(metadataTopicIdInternal)));
            assertThat(uuid, CoreMatchers.not(CoreMatchers.equalTo(zeroIdImpactInternal)));
        }
    }

    @Test
    void testWithKafkaLog() {
        StrimziKafkaContainer kafkaContainer = new StrimziKafkaContainer();

        StrimziKafkaContainer result = kafkaContainer.withKafkaLog(Level.DEBUG);

        assertSame(kafkaContainer, result, "withKafkaLog() should return the same instance for method chaining.");
    }

    @Test
    void testWithServerProperties() {
        StrimziKafkaContainer kafkaContainer = new StrimziKafkaContainer();
        MountableFile serverPropertiesFile = MountableFile.forClasspathResource("server.properties");

        StrimziKafkaContainer result = kafkaContainer.withServerProperties(serverPropertiesFile);

        assertSame(kafkaContainer, result, "withServerProperties() should return the same instance for method chaining.");
    }

    @Test
    void testWithClusterId() {
        StrimziKafkaContainer kafkaContainer = new StrimziKafkaContainer();

        StrimziKafkaContainer result = kafkaContainer.withClusterId("test-cluster-id");

        assertSame(kafkaContainer, result, "withClusterId() should return the same instance for method chaining.");
        assertThat(kafkaContainer.getClusterId(), is("test-cluster-id"));
    }

    @Test
    void testGetBrokerId() {
        StrimziKafkaContainer kafkaContainer = new StrimziKafkaContainer()
            .withBrokerId(5);

        int brokerId = kafkaContainer.getBrokerId();

        assertThat(brokerId, is(5));
    }

    @Test
    void testWithBootstrapServersReturnsSelf() {
        StrimziKafkaContainer kafkaContainer = new StrimziKafkaContainer();

        StrimziKafkaContainer result = kafkaContainer.withBootstrapServers(c -> "PLAINTEXT://localhost:9092");

        assertSame(kafkaContainer, result, "withBootstrapServers() should return the same instance for method chaining.");
    }

    @Test
    void testWithPortAddsFixedExposedPort() {
        StrimziKafkaContainer kafkaContainer = new StrimziKafkaContainer();

        kafkaContainer.withPort(9092);

        assertThat(kafkaContainer.getExposedPorts(), hasItem(9092));
    }

    @Test
    void testGetClusterIdReturnsNullWhenNotSet() {
        StrimziKafkaContainer kafkaContainer = new StrimziKafkaContainer();

        assertThat(kafkaContainer.getClusterId(), nullValue());
    }
}