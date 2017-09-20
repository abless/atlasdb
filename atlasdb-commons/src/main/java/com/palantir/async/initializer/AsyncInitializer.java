/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.async.initializer;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.exception.NotInitializedException;
import com.palantir.logsafe.SafeArg;

/**
 * Implements basic infrastructure to allow an object to be asynchronously initialized.
 * In order to be ThreadSafe, the abstract methods of the inheriting class need to be synchronized.
 */
@ThreadSafe
public abstract class AsyncInitializer {
    private static final Logger log = LoggerFactory.getLogger(AsyncInitializer.class);

    private final ScheduledExecutorService singleThreadedExecutor = Executors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory("AsyncInitializer", true));
    private final AtomicBoolean isInitializing = new AtomicBoolean(false);
    private volatile boolean initialized = false;
    private volatile boolean canceledInitialization = false;

    public final void initialize(boolean initializeAsync) {
        if (!isInitializing.compareAndSet(false, true)) {
            throw new IllegalStateException("Multiple calls tried to initialize the same instance.\n"
                    + "Each instance should have a single thread trying to initialize it.\n"
                    + "Object being initialized multiple times: " + getClassName());
        }

        if (!initializeAsync) {
            tryInitializeInternal();
            return;
        }

        try {
            tryInitializeInternal();
        } catch (Throwable throwable) {
            log.info("Failed to initialize {} in the first attempt, will initialize asynchronously.",
                    SafeArg.of("className", getClassName()), throwable);
            cleanUpOnInitFailure();
            scheduleInitialization();
        }
    }

    private void scheduleInitialization() {
        singleThreadedExecutor.schedule(() -> {
            if (canceledInitialization) {
                return;
            }

            try {
                tryInitializeInternal();
                log.info("Initialized {} asynchronously.", SafeArg.of("className", getClassName()));
            } catch (Throwable throwable) {
                log.info("Failed to initialize {} asynchronously.",
                        SafeArg.of("className", getClassName()), throwable);
                cleanUpOnInitFailure();
                scheduleInitialization();
            }
        }, sleepIntervalInMillis(), TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    protected int sleepIntervalInMillis() {
        return 10_000;
    }

    /**
     * Cancels future initializations and registers a callback to be called if the initialization is happening.
     * If the instance is closeable, it's recommended that the this method is invoked in a close call, and the callback
     * contains a call to the instance's close method.
     */
    protected final void cancelInitialization(Runnable handler) {
        canceledInitialization = true;

        singleThreadedExecutor.submit(() -> {
            if (isInitialized()) {
                handler.run();
            }
        });
    }

    protected final void checkInitialized() {
        if (!initialized) {
            throw new NotInitializedException(getClassName());
        }
    }

    private void tryInitializeInternal() {
        tryInitialize();
        initialized = true;
    }

    public boolean isInitialized() {
        return initialized;
    }

    /**
     * Override this method if there's anything to be cleaned up on initialization failure.
     * Default implementation is no-op.
     */
    protected void cleanUpOnInitFailure() {
        // no-op
    }

    /**
     * Override this method with the calls to initialize an object that may fail.
     * This method will be retried if any exception is thrown on its execution.
     * If there's any follow up action to clean any state left by a previous initialization failure, see
     * {@link AsyncInitializer#cleanUpOnInitFailure}.
     */
    protected abstract void tryInitialize();

    /**
     * This method should contain the wrapped init class
     */
    protected abstract String getClassName();
}