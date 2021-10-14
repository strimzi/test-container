/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.utils;

/**
 * Environment stores env variables
 */
public class Environment {

    // empty constructor to prohibit instantiate object
    private Environment() { }

    /**
     * Strimzi test container kafka version environment variable
     */
    public static final String STRIMZI_TEST_CONTAINER_KAFKA_VERSION_ENV = "STRIMZI_TEST_CONTAINER_KAFKA_VERSION";
    /**
     * Strimzi test container image version environment variable
     */
    public static final String STRIMZI_TEST_CONTAINER_IMAGE_VERSION_ENV = "STRIMZI_TEST_CONTAINER_IMAGE_VERSION";

    /**
     * Wrapper method for fetching env values
     * @param environmentVariableName environment name
     * @return value of specific environment
     */
    public static String getValue(String environmentVariableName) {
        return System.getenv(environmentVariableName);
    }
}
