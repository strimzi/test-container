package io.strimzi.test.container;

import dasniko.testcontainers.keycloak.KeycloakContainer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.core.Core;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.keycloak.admin.client.Keycloak;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;

public class StrimziKafkaOauthIT extends AbstractIT {

    private StrimziKafkaContainer systemUnderTest;

    @Container
    KeycloakContainer keycloakContainer = new KeycloakContainer()
        .withRealmImportFile("/kafka-authz-realm.json")
        .withEnv("KEYCLOAK_ADMIN", "admin")
        .withEnv("KEYCLOAK_ADMIN_PASSWORD", "admin")
        .withExposedPorts(8080)
        .withNetwork(Network.SHARED)
        .withNetworkAliases("keycloak")
        .waitingFor(Wait.forHttp("/realms/master").forStatusCode(200));

    @Test
    void testOAuth() {
        try {
            keycloakContainer.start();

            Keycloak keycloakInstance = this.keycloakContainer.getKeycloakAdminClient();

            String keycloakHost = keycloakContainer.getHost();
            Integer keycloakPort = keycloakContainer.getMappedPort(8080);
            String keycloakAuthUrl = "http://" + keycloakHost + ":" + keycloakPort + "/realms/kafka-authz";

            System.out.println("Keycloak URL:" + keycloakAuthUrl);

            Map<String, String> additionalConfiguration = new HashMap<>();


            // Set Kafka configuration properties suitable for KRaft mode
            additionalConfiguration.put("process.roles", "broker,controller");
            additionalConfiguration.put("node.id", "1");
            additionalConfiguration.put("broker.id", "1");
            additionalConfiguration.put("controller.listener.names", "CONTROLLER");
            additionalConfiguration.put("controller.quorum.voters", "1@localhost:9094");
            additionalConfiguration.put("sasl.enabled.mechanisms", "OAUTHBEARER");
            additionalConfiguration.put("ssl.secure.random.implementation", "SHA1PRNG");
            additionalConfiguration.put("ssl.endpoint.identification.algorithm", "");

            // SASL/OAuth configurations for PLAINTEXT listener
            additionalConfiguration.put("sasl.enabled.mechanisms", "OAUTHBEARER");
            additionalConfiguration.put("listener.name.broker1.oauthbearer.sasl.jaas.config",
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
            additionalConfiguration.put("listener.name.broker1.oauthbearer.sasl.login.callback.handler.class",
                "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");
            additionalConfiguration.put("listener.name.broker1.oauthbearer.sasl.server.callback.handler.class",
                "io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler");

            // OAuth-related configuration
            additionalConfiguration.put("oauth.client.id", "kafka");
            additionalConfiguration.put("oauth.client.secret", "kafka-secret");
            additionalConfiguration.put("oauth.token.endpoint.uri", keycloakAuthUrl + "/protocol/openid-connect/token");
            additionalConfiguration.put("oauth.valid.issuer.uri", keycloakAuthUrl);
            additionalConfiguration.put("oauth.jwks.endpoint.uri", keycloakAuthUrl + "/protocol/openid-connect/certs");
            additionalConfiguration.put("oauth.username.claim", "preferred_username");
            additionalConfiguration.put("oauth.jwks.refresh.min.pause.seconds", "5");

            // Authorizer configuration
            additionalConfiguration.put("authorizer.class.name", "io.strimzi.kafka.oauth.server.authorizer.KeycloakAuthorizer");
            additionalConfiguration.put("strimzi.authorization.token.endpoint.uri", keycloakAuthUrl + "/protocol/openid-connect/token");
            additionalConfiguration.put("strimzi.authorization.client.id", "kafka");
            additionalConfiguration.put("strimzi.authorization.client.secret", "kafka-secret");
            additionalConfiguration.put("strimzi.authorization.kafka.cluster.name", "my-cluster");
            additionalConfiguration.put("strimzi.authorization.delegate.to.kafka.acl", "false");
            additionalConfiguration.put("strimzi.authorization.grants.refresh.period.seconds", "60");

            // Principal builder
            additionalConfiguration.put("principal.builder.class", "io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder");

            // Super users
            additionalConfiguration.put("super.users",
                "User:ANONYMOUS;User:service-account-kafka");

            systemUnderTest = new StrimziKafkaContainer("localhost/strimzi/test-container:latest-kafka-3.8.0-amd64")
                .withKraft()
                .withKafkaConfigurationMap(additionalConfiguration)
                .withNetwork(Network.SHARED)
                .withEnv("KEYCLOAK_HOST", keycloakHost)
                .withEnv("REALM", "kafka-authz")
                .waitForRunning();

            systemUnderTest.start();

            // Now, create a Kafka producer and consumer using OAuth authentication
            // Producer properties
//            Properties producerProps = new Properties();
//            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, systemUnderTest.getBootstrapServers());
//            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//            producerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
//            producerProps.put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER");
//            producerProps.put(SaslConfigs.SASL_JAAS_CONFIG,
//                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
//                    "oauth.client.id=\"team-a-client\" " +
//                    "oauth.client.secret=\"team-a-client-secret\" " +
//                    "oauth.token.endpoint.uri=\"" + keycloakAuthUrl + "/protocol/openid-connect/token\";");
//            producerProps.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
//                "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");
//
//            Producer<String, String> producer = new KafkaProducer<>(producerProps);
//
//            // Send a test message
//            ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", "key", "value");
//            producer.send(record).get();
//
//            producer.close();
//
//            // Consumer properties
//            Properties consumerProps = new Properties();
//            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, systemUnderTest.getBootstrapServers());
//            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//            consumerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
//            consumerProps.put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER");
//            consumerProps.put(SaslConfigs.SASL_JAAS_CONFIG,
//                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
//                    "oauth.client.id=\"team-a-client\" " +
//                    "oauth.client.secret=\"team-a-client-secret\" " +
//                    "oauth.token.endpoint.uri=\"" + keycloakAuthUrl + "/protocol/openid-connect/token\";");
//            consumerProps.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
//                "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");
//            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
//            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//
//            Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
//            consumer.subscribe(Collections.singletonList("test-topic"));
//
//            // Poll for messages
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
//
//            // Verify that the message was received
//            assertThat("No records were received", records.isEmpty(), CoreMatchers.notNullValue());

//            consumer.close();

            System.out.println("==================");
            System.out.println(systemUnderTest.getLogs());
        } finally {
            if (this.keycloakContainer != null) {
                this.keycloakContainer.stop();
            }
            if (this.systemUnderTest != null) {
                this.systemUnderTest.stop();
            }
        }
    }
}
