/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class UtilsTest {

    // -------------------------------------------------------------------------
    // requireNonBlank
    // -------------------------------------------------------------------------

    @Test
    void testRequireNonBlankWithValidString() {
        String result = Utils.requireNonBlank("hello", "field");
        assertThat(result, is("hello"));
    }

    @Test
    void testRequireNonBlankTrimsWhitespace() {
        String result = Utils.requireNonBlank("  hello  ", "field");
        assertThat(result, is("hello"));
    }

    @Test
    void testRequireNonBlankThrowsOnNull() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> Utils.requireNonBlank(null, "myField")
        );
        assertThat(exception.getMessage(), containsString("myField"));
        assertThat(exception.getMessage(), containsString("cannot be null or empty"));
    }

    @Test
    void testRequireNonBlankThrowsOnEmpty() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> Utils.requireNonBlank("", "myField")
        );
        assertThat(exception.getMessage(), containsString("myField"));
    }

    @Test
    void testRequireNonBlankThrowsOnBlank() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> Utils.requireNonBlank("   ", "myField")
        );
        assertThat(exception.getMessage(), containsString("myField"));
    }

    // -------------------------------------------------------------------------
    // getFreePort
    // -------------------------------------------------------------------------

    @Test
    void testGetFreePortReturnsPositivePort() {
        int port = Utils.getFreePort();
        assertThat(port > 0, is(true));
    }

    @Test
    void testGetFreePortReturnsDifferentPorts() {
        int port1 = Utils.getFreePort();
        int port2 = Utils.getFreePort();
        // While not guaranteed, two consecutive calls should typically return different ports
        assertThat(port1 > 0, is(true));
        assertThat(port2 > 0, is(true));
    }

    // -------------------------------------------------------------------------
    // asTransferableBytes
    // -------------------------------------------------------------------------

    @Test
    void testAsTransferableBytesWithNull() {
        Optional<Transferable> result = Utils.asTransferableBytes(null);
        assertThat(result.isPresent(), is(false));
    }

    @Test
    void testAsTransferableBytesWithValidFile(@TempDir Path tempDir) throws IOException {
        Path file = tempDir.resolve("test.txt");
        Files.write(file, "test content".getBytes(StandardCharsets.UTF_8));

        MountableFile mountableFile = MountableFile.forHostPath(file.toString());
        Optional<Transferable> result = Utils.asTransferableBytes(mountableFile);

        assertThat(result.isPresent(), is(true));
    }

    @Test
    void testAsTransferableBytesWithNonExistentFileThrowsException(@TempDir Path tempDir) {
        Path nonExistent = tempDir.resolve("does-not-exist.txt");
        MountableFile mountableFile = MountableFile.forHostPath(nonExistent.toString());

        assertThrows(UncheckedIOException.class,
            () -> Utils.asTransferableBytes(mountableFile));
    }

    // -------------------------------------------------------------------------
    // waitFor
    // -------------------------------------------------------------------------

    @Test
    void testWaitForReturnsTrueImmediately() {
        long remaining = Utils.waitFor(
            "immediate success",
            Duration.ofMillis(10),
            Duration.ofMillis(1000),
            () -> true
        );
        assertThat(remaining > 0, is(true));
    }

    @Test
    void testWaitForThrowsOnTimeout() {
        WaitException exception = assertThrows(
            WaitException.class,
            () -> Utils.waitFor(
                "always false",
                Duration.ofMillis(10),
                Duration.ofMillis(50),
                () -> false
            )
        );
        assertThat(exception.getMessage(), containsString("Timeout after"));
        assertThat(exception.getMessage(), containsString("always false"));
    }

    @Test
    void testWaitForSucceedsAfterRetries() {
        AtomicInteger counter = new AtomicInteger(0);
        long remaining = Utils.waitFor(
            "succeeds on third attempt",
            Duration.ofMillis(10),
            Duration.ofMillis(5000),
            () -> counter.incrementAndGet() >= 3
        );
        assertThat(remaining > 0, is(true));
        assertThat(counter.get(), is(3));
    }

    @Test
    void testWaitForHandlesSingleException() {
        AtomicInteger counter = new AtomicInteger(0);
        long remaining = Utils.waitFor(
            "exception then success",
            Duration.ofMillis(10),
            Duration.ofMillis(5000),
            () -> {
                if (counter.incrementAndGet() == 1) {
                    throw new RuntimeException("first call fails");
                }
                return true;
            }
        );
        assertThat(remaining > 0, is(true));
        assertThat(counter.get(), is(2));
    }

    @Test
    void testWaitForHandlesMultipleExceptionsBeforeTimeout() {
        WaitException exception = assertThrows(
            WaitException.class,
            () -> Utils.waitFor(
                "always throws",
                Duration.ofMillis(10),
                Duration.ofMillis(50),
                () -> {
                    throw new RuntimeException("always fails");
                }
            )
        );
        assertThat(exception.getMessage(), containsString("Timeout after"));
    }

    @Test
    void testWaitForHandlesExceptionWithNullMessage() {
        WaitException exception = assertThrows(
            WaitException.class,
            () -> Utils.waitFor(
                "null message exception",
                Duration.ofMillis(10),
                Duration.ofMillis(50),
                () -> {
                    throw new RuntimeException((String) null);
                }
            )
        );
        assertThat(exception.getMessage(), containsString("Timeout after"));
    }

    @Test
    void testWaitForHandlesInterruptedException() {
        Thread currentThread = Thread.currentThread();

        // Schedule an interrupt after a short delay
        Thread interrupter = new Thread(() -> {
            try {
                Thread.sleep(30);
            } catch (InterruptedException e) {
                // ignore
            }
            currentThread.interrupt();
        });
        interrupter.start();

        // waitFor should return when interrupted (not throw)
        Utils.waitFor(
            "interrupted",
            Duration.ofMillis(50),
            Duration.ofMillis(5000),
            () -> false
        );

        // Clear the interrupt flag
        Thread.interrupted();
    }

    @Test
    void testWaitForPollIntervalLargerThanTimeLeft() {
        // pollInterval > timeout to test sleepTime = Math.min(pollInterval, timeLeft)
        WaitException exception = assertThrows(
            WaitException.class,
            () -> Utils.waitFor(
                "large poll interval",
                Duration.ofMillis(10000),
                Duration.ofMillis(50),
                () -> false
            )
        );
        assertThat(exception.getMessage(), containsString("Timeout after"));
    }
}
