/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *  A logical Kafka version
 */
public class LogicalKafkaVersionEntity {

    private static final Logger LOGGER = LogManager.getLogger(LogicalKafkaVersionEntity.class);

    private static final Pattern STRIMZI_TEST_CONTAINER_IMAGE_WITHOUT_KAFKA_VERSION = Pattern.compile("^test-container:(\\d+\\.\\d+\\.\\d+|latest)-kafka-.*$");
    private static final String KAFKA_VERSIONS_URL_JSON = "https://raw.githubusercontent.com/strimzi/test-container-images/main/kafka_versions.yaml"; // this would be replaced by test-container-images url where json will be stored

    private String jsonVersion;
    private final List<LogicalKafkaVersion> logicalKafkaVersionEntities = new ArrayList<>();

    public LogicalKafkaVersionEntity() {
        // scrape json schema and fill the inner list of versions
        this.resolveAndParse();
    }

    public static class LogicalKafkaVersion implements Comparable<LogicalKafkaVersion> {
        private final String version;
        private final String image;

        public LogicalKafkaVersion(String version, String image) {
            this.version = version;
            this.image = image;
        }

        @Override
        public int compareTo(LogicalKafkaVersion o) {
            return compareVersions(this.version, o.version);
        }

        /**
         * Compare two decimal version strings, e.g. 1.10.1 &gt; 1.9.2
         * @param version1 The first version.
         * @param version2 The second version.
         * @return 0 if version1 == version2;
         * -1 if version1 &lt; version2;
         * 1 if version1 &gt; version2.
         */
        public static int compareVersions(String version1, String version2) {
            String[] components = version1.split("\\.");
            String[] otherComponents = version2.split("\\.");
            for (int i = 0; i < Math.min(components.length, otherComponents.length); i++) {
                int x = Integer.parseInt(components[i]);
                int y = Integer.parseInt(otherComponents[i]);
                // equality of version is ignored...
                if (x < y) {
                    return -1;
                } else if (x > y) {
                    return 1;
                }

            }
            return components.length - otherComponents.length;
        }

        // f.e. "3.0.0"
        public String getVersion() {
            return version;
        }

        // f.e. "test-container:0.1.0-kafka-3.0.0
        public String getImage() {
            return image;
        }

        public String getStrimziTestContainerVersion() {
            Matcher regex = STRIMZI_TEST_CONTAINER_IMAGE_WITHOUT_KAFKA_VERSION.matcher(this.image);

            // only one occurrence in the string
            if (regex.find()) {
                final String strimziContainerVersion = regex.group(1);
                LOGGER.info("Find the version of Strimzi kafka container:{}", strimziContainerVersion);
                return strimziContainerVersion;
            } else {
                throw new RuntimeException("Unable to find version of Strimzi kafka container from image name - " + this.image);
            }
        }

        @Override
        public String toString() {
            return "LogicalKafkaVersionEntity{" +
                "version='" + this.version + '\'' +
                ", image='" + this.image + '\'' +
                '}';
        }
    }

    /**
     * Get the latest release
     * @return LogicalKafkaVersion latest release
     */
    public LogicalKafkaVersion latestRelease() {
        // at least one release in the json schema is needed
        if (this.logicalKafkaVersionEntities == null || this.logicalKafkaVersionEntities.size() < 1) {
            throw new IllegalStateException("Wrong json schema! It must have at least one release");
        }

        LogicalKafkaVersion latestRelease = this.logicalKafkaVersionEntities.get(0);

        for (int i = 1; i < this.logicalKafkaVersionEntities.size(); i++) {
            if (latestRelease.compareTo(this.logicalKafkaVersionEntities.get(i)) < 0) {
                latestRelease = this.logicalKafkaVersionEntities.get(i);
            }
        }

        LOGGER.info("Latest release of Kafka is:{}", latestRelease.toString());

        return latestRelease;
    }

    /**
     * Gets the previous minor release.
     * If this instance were 2.8.1 then at a certain point in time this method
     * might return a version reflecting 2.7.2.
     * On a later date it might return 2.7.3.
     * It is allowed to cross major version boundaries.
     * E.g. the previous minor release to 3.0.0 might be 2.8.2
     * @return LogicalKafkaVersion the previous minor release
     */
    public LogicalKafkaVersion previousMinor() {
        if (this.logicalKafkaVersionEntities == null || this.logicalKafkaVersionEntities.size() < 1) {
            throw new IllegalStateException("Wrong json schema! It must have at least one release");
        }

        // sort in this case always order the latest release at the end so the previous one is the previous minor release
        this.logicalKafkaVersionEntities.sort(LogicalKafkaVersion::compareTo);

        // 1.0.0
        // 2.40.50
        // 3.0.0 <- previous minor (it will be always previous last)
        // 4.1.2 <- latest release
        final LogicalKafkaVersion previousMinorRelease = this.logicalKafkaVersionEntities.get(this.logicalKafkaVersionEntities.size() - 2);
        LOGGER.info("Previous minor release of Kafka is:{}", previousMinorRelease);
        return previousMinorRelease;
    }

    /**
     * Resolve the logical version to an actual version
     */
    private void resolveAndParse() {
        // Connect to the URL using java's native library
        try {
            StringWriter writer = new StringWriter();
            IOUtils.copy(new URL(KAFKA_VERSIONS_URL_JSON).openStream(), writer, StandardCharsets.UTF_8);
            String pureStringJson = writer.toString().replaceAll("(?m)^#.*(?:\\r?\\n)", "");
            JsonNode rootNode = new ObjectMapper().readValue(pureStringJson, JsonNode.class);

            this.jsonVersion = rootNode.get("version").toString();

            for (Iterator<Map.Entry<String, JsonNode>> iter = rootNode.get("kafkaVersions").fields(); iter.hasNext(); ) {
                Map.Entry<String, JsonNode> fields = iter.next();
                LogicalKafkaVersion logicalKafkaVersion = new LogicalKafkaVersion(fields.getKey(), fields.getValue().asText());
                this.logicalKafkaVersionEntities.add(logicalKafkaVersion);
            }
        } catch (IOException e) {
            LOGGER.error("Error occurred during instantiation of JsonReader!", e);
        }
    }

    @Override
    public String toString() {
        return "LogicalKafkaVersion{" +
            "jsonVersion='" + jsonVersion + '\'' +
            ", logicalKafkaVersionEntities=" + logicalKafkaVersionEntities.toString() +
            '}';
    }
}
