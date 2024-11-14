/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.BooleanSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.MountableFile;

/**
 * Utils contains auxiliary static methods, which are used in whole project.
 */
class Utils {

    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

    /**
     * Poll the given {@code ready} function every {@code pollIntervalMs} milliseconds until it returns true,
     * or throw a WaitException if it doesn't returns true within {@code timeoutMs} milliseconds.
     * @return The remaining time left until timeout occurs
     * (helpful if you have several calls which need to share a common timeout),
     *
     * @param description waiting for `description`
     * @param pollIntervalMs poll interval in milliseconds
     * @param timeoutMs timeout in milliseconds
     * @param ready lambda predicate
     */
    static long waitFor(String description, long pollIntervalMs, long timeoutMs, BooleanSupplier ready) {
        LOGGER.debug("Waiting for {}", description);
        long deadline = System.currentTimeMillis() + timeoutMs;
        String exceptionMessage = null;
        int exceptionCount = 0;
        StringWriter stackTraceError = new StringWriter();

        while (true) {
            boolean result;
            try {
                result = ready.getAsBoolean();
            } catch (Exception e) {
                exceptionMessage = e.getMessage();
                if (++exceptionCount == 1 && exceptionMessage != null) {
                    // Log the first exception as soon as it occurs
                    LOGGER.error("Exception waiting for {}, {}", description, exceptionMessage);
                    // log the stacktrace
                    e.printStackTrace(new PrintWriter(stackTraceError));
                }
                result = false;
            }
            long timeLeft = deadline - System.currentTimeMillis();
            if (result) {
                return timeLeft;
            }
            if (timeLeft <= 0) {
                if (exceptionCount > 1) {
                    LOGGER.error("Exception waiting for {}, {}", description, exceptionMessage);

                    if (!stackTraceError.toString().isEmpty()) {
                        // printing handled stacktrace
                        LOGGER.error(stackTraceError.toString());
                    }
                }
                WaitException waitException = new WaitException("Timeout after " + timeoutMs + " ms waiting for " + description);
                waitException.addSuppressed(waitException);
                throw waitException;
            }
            long sleepTime = Math.min(pollIntervalMs, timeLeft);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("{} not satisfied, will try again in {} ms ({}ms till timeout)", description, sleepTime, timeLeft);
            }
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                return deadline - System.currentTimeMillis();
            }
        }
    }

    static long waitFor(String description, BooleanSupplier ready) {
        return waitFor(description, 1_000, 30_000, ready);
    }

    /**
     * Converts the contents of {@code file} to a new Transferable. If the file is
     * null, an empty Optional is returned. This method reads the contents of the
     * file to avoid preservation of the file's owner and group attributes when
     * copying into the container.
     *
     * @param file the source file
     * @return an Optional containing the Transferable contents of file, or an empty
     *         Optional when the file is null.
     */
    static Optional<Transferable> asTransferableBytes(MountableFile file) {
        if (file != null) {
            final byte[] data;

            try {
                data = Files.readAllBytes(Path.of(file.getFilesystemPath()));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            return Optional.of(Transferable.of(data));
        }

        return Optional.empty();
    }

    /**
     * Finds a free server port which can be used by the web server
     *
     * @return A free TCP port
     */
    public static int getFreePort()   {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to find free port", e);
        }
    }
}
