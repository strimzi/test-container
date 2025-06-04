/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

/**
 * Indicates that a method should be excluded from mutation testing within the test container.
 * <p>
 * This annotation is intended for methods that are specifically covered by integration tests
 * and cannot be effectively tested at the unit test level.
 * Mutation testing tools should skip mutating these annotated methods to avoid false negatives
 * in mutation coverage reporting.
 *
 * <p><b>Note:</b> This annotation is <i>not</i> part of the public API and is for internal use only.</p>
 */
@interface DoNotMutate {
}
