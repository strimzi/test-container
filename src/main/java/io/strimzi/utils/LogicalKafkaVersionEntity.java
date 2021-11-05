/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.utils;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *  A logical Kafka version
 */
public class LogicalKafkaVersionEntity {

    private static final Logger LOGGER = LogManager.getLogger(LogicalKafkaVersionEntity.class);

    private static final Pattern STRIMZI_TEST_CONTAINER_IMAGE_WITHOUT_KAFKA_VERSION = Pattern.compile("^test-container:(\\d+\\.\\d+\\.\\d+|latest)-kafka-.*$");
    private static final String KAFKA_VERSIONS_URL_JSON = "https://api.jsonbin.io/b/PLACEHOLDER"; // this would be replaced by test-container-images url where json will be stored
    private static JsonReader reader;

    private String jsonVersion;
    private List<LogicalKafkaVersion> logicalKafkaVersionEntities = new ArrayList<>();

    public LogicalKafkaVersionEntity() throws IOException {
        // scrape json schema and fill the inner list of versions
        this.resolve();
    }

    public static class LogicalKafkaVersion implements Comparable<LogicalKafkaVersion> {
        private final String version;
        private final String image;

        public LogicalKafkaVersion(String version, String image) {
            this.version = version;
            this.image = image;
        }

        public static LogicalKafkaVersion fromJson(JsonReader reader) throws IOException {
            return new LogicalKafkaVersion(reader.nextName(), reader.nextString());
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
            Matcher regex = STRIMZI_TEST_CONTAINER_IMAGE_WITHOUT_KAFKA_VERSION.matcher(image);

            // only one occurrence in the string
            if (regex.find()) {
                final String strimziContainerVersion = regex.group(1);
                LOGGER.info("Find the version of Strimzi kafka container:{}", strimziContainerVersion);
                return strimziContainerVersion;
            } else {
                throw new RuntimeException("Unable to find version of Strimzi kafka container from image name - " + image);
            }
        }

        @Override
        public String toString() {
            return "LogicalKafkaVersionEntity{" +
                "version='" + version + '\'' +
                ", image='" + image + '\'' +
                '}';
        }
    }

    /**
     * Get the latest release
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
        LOGGER.error(this.logicalKafkaVersionEntities.toString());
        final LogicalKafkaVersion previousMinorRelease = this.logicalKafkaVersionEntities.get(this.logicalKafkaVersionEntities.size() - 2);
        LOGGER.info("Previous minor release of Kafka is:{}", previousMinorRelease);
        return previousMinorRelease;
    }

    /**
     * Resolve the logical version to an actual version
     */
    private void resolve() throws IOException {
        // Connect to the URL using java's native library
        URLConnection request;
        try {
            request = new URL(KAFKA_VERSIONS_URL_JSON).openConnection();
            request.addRequestProperty("Secret-key", "----PLACE...");
            request.connect();

            reader = new JsonReader(new InputStreamReader(request.getInputStream(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }

        reader.beginObject();

        // version
        reader.nextName();
        this.jsonVersion = String.valueOf(reader.nextInt());

        // kafka-versions
        reader.nextName();
        reader.beginObject();

        // pre-allocate 2 items and eliminate re-allocation
        this.logicalKafkaVersionEntities = new ArrayList<>(2);

        while (reader.peek() != JsonToken.END_OBJECT) {
            this.logicalKafkaVersionEntities.add(LogicalKafkaVersion.fromJson(reader));
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
