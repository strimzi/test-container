package io.strimzi.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * AuxiliaryVariables contains serveral variables, which are used in whole project. Moreover it also has nested class
 * @see{Environment}, which stores all supported env variables.
 */
public class AuxiliaryVariables {

    private AuxiliaryVariables() { }

    public static String STRIMZI_TEST_CONTAINER_IMAGE_VERSION = Utils.getStrimziTestContainerVersion();

    /**
     * Environment stores env variables
     */
    public static class Environment {

        private static final Logger LOGGER = LogManager.getLogger(Environment.class);
        private static final Map<String, String> VALUES = new HashMap<>();
        private static final JsonNode JSON_DATA = loadConfigurationFile();

        static {
            String debugFormat = "{}: {}";
            LOGGER.info("Used environment variables:");
            VALUES.forEach((key, value) -> LOGGER.info(debugFormat, key, value));
        }

        // empty constructor to prohibit instantiate object
        private Environment() { }

        // env variables
        private static final String CONFIG_FILE_PATH_ENV = "ST_CONFIG_PATH";
        private static final String STRIMZI_TEST_CONTAINER_KAFKA_VERSION_ENV = "STRIMZI_TEST_CONTAINER_KAFKA_VERSION";

        // env variable default values
        private static final String STRIMZI_TEST_CONTAINER_KAFKA_VERSION_DEFAULT = "3.0.0";

        // set values
        public static final String STRIMZI_TEST_CONTAINER_KAFKA_VERSION = getOrDefault(STRIMZI_TEST_CONTAINER_KAFKA_VERSION_ENV, STRIMZI_TEST_CONTAINER_KAFKA_VERSION_DEFAULT);

        private static String getOrDefault(String varName, String defaultValue) {
            return getOrDefault(varName, String::toString, defaultValue);
        }

        private static <T> T getOrDefault(String var, Function<String, T> converter, T defaultValue) {
            String value = System.getenv(var) != null ?
                System.getenv(var) :
                (Objects.requireNonNull(JSON_DATA).get(var) != null ?
                    JSON_DATA.get(var).asText() :
                    null);
            T returnValue = defaultValue;
            if (value != null) {
                returnValue = converter.apply(value);
            }
            VALUES.put(var, String.valueOf(returnValue));
            return returnValue;
        }

        private static JsonNode loadConfigurationFile() {
            final String config = System.getenv().getOrDefault(CONFIG_FILE_PATH_ENV,
                Paths.get(System.getProperty("user.dir"), "config.json").toAbsolutePath().toString());
            ObjectMapper mapper = new ObjectMapper();
            try {
                File jsonFile = new File(config).getAbsoluteFile();
                return mapper.readTree(jsonFile);
            } catch (IOException ex) {
                LOGGER.info("Json configuration is not provided or cannot be processed");
                return mapper.createObjectNode();
            }
        }
    }
}
