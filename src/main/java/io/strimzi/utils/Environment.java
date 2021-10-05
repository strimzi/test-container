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

    // env variables
    public static final String STRIMZI_TEST_CONTAINER_KAFKA_VERSION_ENV = "STRIMZI_TEST_CONTAINER_KAFKA_VERSION";
    public static final String STRIMZI_TEST_CONTAINER_IMAGE_VERSION_ENV = "STRIMZI_TEST_CONTAINER_IMAGE_VERSION";

    public static String getValue(String environmentVariableName) {
        return System.getenv(environmentVariableName);
    }
}
