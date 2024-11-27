/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import java.time.Duration;
import java.util.function.BooleanSupplier;

/**
 * Extension for RuntimeException used in active waiting. See the
 * {@link Utils#waitFor(String, Duration, Duration, BooleanSupplier)} method.
 * Usage of this Exception should be always associated with active waiting where the condition
 * should not always be met which results in WaitException.
 */
public class WaitException extends RuntimeException {

    /**
     *  WaitException used for active waiting
     * @param message specific message to throw
     */
    public WaitException(String message) {
        super(message);
    }

    /**
     * WaitException used for active waiting
     * @param cause type of cause
     */
    public WaitException(Throwable cause) {
        super(cause);
    }
}