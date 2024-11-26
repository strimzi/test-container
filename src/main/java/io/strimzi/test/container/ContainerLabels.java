/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * An immutable set of labels for containers.
 */
class ContainerLabels {
    /**
     * Strimzi domain used for labels.
     */
    public static final String STRIMZI_DOMAIN = "strimzi.io/";

    // Label keys
    public static final String STRIMZI_TEST_CONTAINER_LABEL = STRIMZI_DOMAIN + "test-container";
    public static final String STRIMZI_TEST_ID_LABEL = STRIMZI_DOMAIN + "test-id";

    private final Map<String, String> labels;

    /**
     * Constructs an empty set of labels.
     */
    public ContainerLabels() {
        this.labels = Collections.emptyMap();
    }

    private ContainerLabels(Map<String, String> labels) {
        this.labels = Collections.unmodifiableMap(new HashMap<>(labels));
    }

    /**
     * Adds a label to the current set of labels.
     *
     * @param key   the label key
     * @param value the label value
     * @return a new ContainerLabels instance with the added label
     */
    public ContainerLabels withLabel(String key, String value) {
        Map<String, String> newLabels = new HashMap<>(this.labels);
        newLabels.put(key, value);
        return new ContainerLabels(newLabels);
    }

    /**
     * Returns an unmodifiable map of the labels.
     *
     * @return the labels map
     */
    public Map<String, String> toMap() {
        return labels;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ContainerLabels that = (ContainerLabels) o;
        return Objects.equals(labels, that.labels);
    }

    @Override
    public int hashCode() {
        return labels != null ? labels.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "ContainerLabels" + labels;
    }
}