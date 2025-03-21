/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.params.provider.Arguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

public class AbstractIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractIT.class);

    /**
     * Provides {@code parameters} for each {@link org.junit.jupiter.params.ParameterizedTest}.
     *
     * @return stream of arguments, in this case only image from {@code /kafka_versions.json} file
     * @throws IOException exception
     */
    protected static Stream<Arguments> retrieveKafkaVersionsFile() throws IOException {
        final JsonNode rootNode = new ObjectMapper().readValue(KafkaVersionService.class.getResourceAsStream("/kafka_versions.json"), JsonNode.class);
        final List<Arguments> parameters = new LinkedList<>();

        for (Iterator<Map.Entry<String, JsonNode>> iter = rootNode.get("kafkaVersions").fields(); iter.hasNext(); ) {
            final Map.Entry<String, JsonNode> fields = iter.next();
            parameters.add(Arguments.of(fields.getValue().asText(), fields.getKey()));
        }
        return parameters.stream();
    }

    protected boolean isLessThanKafka400(final String kafkaVersion) {
        return KafkaVersionService.KafkaVersion.compareVersions(kafkaVersion, "4.0.0") == -1;
    }
}
