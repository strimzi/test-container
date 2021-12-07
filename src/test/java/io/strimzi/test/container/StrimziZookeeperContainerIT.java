/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import io.strimzi.utils.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class StrimziZookeeperContainerIT {

    private static final Logger LOGGER = LogManager.getLogger(StrimziZookeeperContainerIT.class);

    private StrimziZookeeperContainer systemUnderTest;
    private StrimziKafkaContainer kafkaContainer;

    @Test
    void testZookeeperContainerStartup() {
        try {
            systemUnderTest = new StrimziZookeeperContainer();
            systemUnderTest.start();

            final String zookeeperLogs = systemUnderTest.getLogs();

            assertThat(zookeeperLogs, notNullValue());
            assertThat(zookeeperLogs, containsString("Created server"));
        } finally {
            systemUnderTest.stop();
        }
    }

    @Test
    void testZookeeperWithKafkaContainer() {
        try {
            systemUnderTest = new StrimziZookeeperContainer();
            systemUnderTest.start();

            Map<String, String> config = new HashMap<>();
            config.put("zookeeper.connect", "zookeeper:2181");

            kafkaContainer = new StrimziKafkaContainer()
                .withBrokerId(1)
                .withKafkaConfigurationMap(config)
                .withExternalZookeeperConnect("zookeeper:" + Constants.ZOOKEEPER_PORT);

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
