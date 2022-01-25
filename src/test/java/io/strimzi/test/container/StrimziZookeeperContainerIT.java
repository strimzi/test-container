/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class StrimziZookeeperContainerIT extends AbstractIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(StrimziZookeeperContainerIT.class);

    private StrimziZookeeperContainer systemUnderTest;
    private StrimziKafkaContainer kafkaContainer;

    @ParameterizedTest(name = "testZookeeperContainerStartup-{0}")
    @MethodSource("retrieveKafkaVersionsFile")
    void testZookeeperContainerStartup(final String imageName) {
        try {
            systemUnderTest = new StrimziZookeeperContainer(imageName);
            systemUnderTest.start();

            final String zookeeperLogs = systemUnderTest.getLogs();

            assertThat(zookeeperLogs, notNullValue());
            assertThat(zookeeperLogs, containsString("Created server"));
        } finally {
            systemUnderTest.stop();
        }
    }

    @ParameterizedTest(name = "testZookeeperWithKafkaContainer-{0}")
    @MethodSource("retrieveKafkaVersionsFile")
    void testZookeeperWithKafkaContainer(final String imageName) {
        try {
            systemUnderTest = new StrimziZookeeperContainer(imageName);
            systemUnderTest.start();

            Map<String, String> config = new HashMap<>();
            config.put("zookeeper.connect", "zookeeper:2181");

            kafkaContainer = new StrimziKafkaContainer()
                .withBrokerId(1)
                .withKafkaConfigurationMap(config)
                .withExternalZookeeperConnect("zookeeper:" + StrimziZookeeperContainer.ZOOKEEPER_PORT);

            kafkaContainer.start();

            final String kafkaLogs = kafkaContainer.getLogs();

            assertThat(kafkaLogs, notNullValue());
            assertThat(kafkaLogs, containsString("Initiating client connection"));
            // kafka established connection to external zookeeper
            assertThat(kafkaLogs, containsString("Session establishment complete on server zookeeper"));
        } finally {
            kafkaContainer.stop();
            systemUnderTest.stop();
        }
    }
}
