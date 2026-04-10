// Copyright (c) 2024 The Chromium Embedded Framework Authors. All rights
// reserved. Use of this source code is governed by a BSD-style license that
// can be found in the LICENSE file.

package org.cef.browser;

import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class CefDevToolsClient implements AutoCloseable {
    private static final AtomicLong NEXT_CALL_ID_ = new AtomicLong();

    private final Map<Integer, CompletableFuture<String>> queuedCommands_ =
            Collections.synchronizedMap(new HashMap<>());
    private final Set<EventListener> eventListeners_ =
            Collections.synchronizedSet(new LinkedHashSet<>());
    private final Map<Integer, Long> messageIdsToCallIds_ = new ConcurrentHashMap<>();
    private final Map<Long, String> callIdsToMethods_ = new ConcurrentHashMap<>();
    private CefRegistration registration_;
    private final CefBrowser_N browser_;

    /**
     * Use {@link CefBrowser#getDevToolsClient()} to get an instance of this class.
     */
    CefDevToolsClient(CefBrowser_N browser) {
        this.browser_ = browser;

        registration_ = browser.addDevToolsMessageObserver(new CefDevToolsMessageObserver() {
            @Override
            public void onDevToolsMethodResult(
                    CefBrowser browser, int messageId, boolean success, String result) {
            Long callId = messageIdsToCallIds_.get(messageId);
            String method = callId != null ? callIdsToMethods_.get(callId) : null;
            CompletableFuture<String> future =
                getQueuedCommand(messageId, "observer", callId, method);
            logEvent(
                "observer-method-result",
                callId,
                messageId,
                method,
                "success="
                    + success
                    + " resultLength="
                    + safeLength(result)
                    + " resultPreview=\""
                    + abbreviate(result)
                    + "\" futureId="
                    + futureId(future)
                    + " mapSizeBeforeCompletion="
                    + queuedCommandsSize());
                if (success) {
                    future.complete(result);
                } else {
                    future.completeExceptionally(
                            new DevToolsException("DevTools method failed", result));
                }
            logEvent(
                "observer-method-result-completed",
                callId,
                messageId,
                method,
                "success="
                    + success
                    + " futureId="
                    + futureId(future)
                    + " mapSizeAfterCompletion="
                    + queuedCommandsSize());
            removeQueuedCommand(messageId, "observer", callId, method);
            }

            @Override
            public void onDevToolsEvent(CefBrowser browser, String method, String parameters) {
                for (EventListener eventListener : eventListeners_) {
                    eventListener.onEvent(method, parameters);
                }
            }
        });
    }

    @Override
    public void close() {
        queuedCommands_.clear();
        eventListeners_.clear();
        messageIdsToCallIds_.clear();
        callIdsToMethods_.clear();
        registration_ = null;
    }

    public boolean isClosed() {
        return registration_ == null;
    }

    private CompletableFuture<String> getQueuedCommand(Integer messageId) {
        return getQueuedCommand(messageId, "unknown", null, null);
    }

    private void removeQueuedCommand(Integer messageId) {
        removeQueuedCommand(messageId, "unknown", null, null);
    }

    private CompletableFuture<String> getQueuedCommand(
            Integer messageId, String source, Long callId, String method) {
        CompletableFuture<String> future;
        boolean created = false;
        int mapSize;
        synchronized (queuedCommands_) {
            future = queuedCommands_.get(messageId);
            if (future == null) {
                future = new CompletableFuture<>();
                queuedCommands_.put(messageId, future);
                created = true;
            }
            mapSize = queuedCommands_.size();
        }

        Long resolvedCallId = callId != null ? callId : messageIdsToCallIds_.get(messageId);
        String resolvedMethod =
                method != null
                        ? method
                        : (resolvedCallId != null ? callIdsToMethods_.get(resolvedCallId) : null);
        logEvent(
                "queued-command-lookup",
                resolvedCallId,
                messageId,
                resolvedMethod,
                "source="
                        + source
                        + " action="
                        + (created ? "created" : "reused")
                        + " futureId="
                        + futureId(future)
                        + " mapSize="
                        + mapSize);
        if (created && "observer".equals(source)) {
            logEvent(
                    "warning",
                    resolvedCallId,
                    messageId,
                    resolvedMethod,
                    "message=\"DevTools result arrived before consumer bound queued future\" futureId="
                            + futureId(future));
        }
        return future;
    }

    private void removeQueuedCommand(
            Integer messageId, String source, Long callId, String method) {
        CompletableFuture<String> removedFuture;
        int mapSize;
        synchronized (queuedCommands_) {
            removedFuture = queuedCommands_.remove(messageId);
            mapSize = queuedCommands_.size();
        }

        Long resolvedCallId = callId != null ? callId : messageIdsToCallIds_.get(messageId);
        String resolvedMethod =
                method != null
                        ? method
                        : (resolvedCallId != null ? callIdsToMethods_.get(resolvedCallId) : null);
        logEvent(
                "remove-queued-command",
                resolvedCallId,
                messageId,
                resolvedMethod,
                "source="
                        + source
                        + " existed="
                        + (removedFuture != null)
                        + " removedFutureId="
                        + futureId(removedFuture)
                        + " mapSize="
                        + mapSize);
    }

    /**
     * Execute a method call over the DevTools protocol. See the <a
     * href=https://chromedevtools.github.io/devtools-protocol/> DevTools protocol documentation</a>
     * for details of supported methods and the expected syntax for parameters.
     *
     * <p>If an error occurs the returned future is completed exceptionally, otherwise its value is
     * asynchronously set to the method result.
     *
     * <p>Call {@link #addEventListener(String, EventListener)} to subscribe to events.
     *
     * @param method the method name
     * @return return a future with the method result if the method was executed successfully
     */
    public CompletableFuture<String> executeDevToolsMethod(String method) {
        return executeDevToolsMethod(method, null);
    }

    /**
     * Execute a method call over the DevTools protocol. See the <a
     * href=https://chromedevtools.github.io/devtools-protocol/> DevTools protocol documentation</a>
     * for details of supported methods and the expected syntax for parameters.
     *
     * <p>If an error occurs the returned future is completed exceptionally, otherwise its value is
     * asynchronously set to the method result.
     *
     * <p>Call {@link #addEventListener(String, EventListener)} to subscribe to events.
     *
     * @param method the method name
     * @param parametersAsJson JSON object with parameters, or null if no parameters are needed
     * @return return a future with the method result if the method was executed successfully
     */
    public CompletableFuture<String> executeDevToolsMethod(String method, String parametersAsJson) {
        long callId = NEXT_CALL_ID_.incrementAndGet();
        callIdsToMethods_.put(callId, method);
        logEvent(
                "execute-requested",
                callId,
                null,
                method,
                "closed="
                        + isClosed()
                        + " hasParams="
                        + (parametersAsJson != null)
                        + " paramsLength="
                        + safeLength(parametersAsJson));
        if (isClosed()) {
            CompletableFuture<String> future = new CompletableFuture<>();
            future.completeExceptionally(new DevToolsException("Client is closed"));
            logEvent(
                    "public-future-completed",
                    callId,
                    null,
                    method,
                    "success=false exceptionType=" + DevToolsException.class.getSimpleName());
            callIdsToMethods_.remove(callId);
            return future;
        }

        AtomicReference<Integer> messageIdRef = new AtomicReference<>();
        CompletableFuture<String> result =
                browser_.executeDevToolsMethod(method, parametersAsJson)
                .whenComplete((messageId, messageIdError) -> {
                    if (messageIdError != null) {
                        logEvent(
                                "native-message-id-future-completed",
                                callId,
                                null,
                                method,
                                "success=false exceptionType="
                                        + messageIdError.getClass().getSimpleName());
                        return;
                    }

                    messageIdRef.set(messageId);
                    messageIdsToCallIds_.put(messageId, callId);
                    logEvent(
                            "native-message-id-future-completed",
                            callId,
                            messageId,
                            method,
                            "success=true");
                })
                .thenCompose(messageId -> getQueuedCommand(messageId, "consumer", callId, method));
        return result.whenComplete(
                (value, error) -> {
                    Integer messageId = messageIdRef.get();
                    logEvent(
                            "public-future-completed",
                            callId,
                            messageId,
                            method,
                            "success="
                                    + (error == null)
                                    + (error != null
                                            ? " exceptionType="
                                                    + error.getClass().getSimpleName()
                                            : " resultLength=" + safeLength(value)));
                    if (messageId != null) {
                        messageIdsToCallIds_.remove(messageId);
                    }
                    callIdsToMethods_.remove(callId);
                });
    }

    private int queuedCommandsSize() {
        synchronized (queuedCommands_) {
            return queuedCommands_.size();
        }
    }

    private static int futureId(CompletableFuture<?> future) {
        return future != null ? System.identityHashCode(future) : -1;
    }

    private static int safeLength(String value) {
        return value != null ? value.length() : -1;
    }

    private static String abbreviate(String value) {
        if (value == null) {
            return "null";
        }
        String singleLine = value.replace('\n', ' ').replace('\r', ' ');
        if (singleLine.length() <= 120) {
            return singleLine;
        }
        return singleLine.substring(0, 120) + "...";
    }

    private static void logEvent(
            String event, Long callId, Integer messageId, String method, String details) {
        StringBuilder message = new StringBuilder("[JCEF-DEVTOOLS] event=").append(event);
        if (callId != null) {
            message.append(" callId=").append(callId);
        }
        if (messageId != null) {
            message.append(" messageId=").append(messageId);
        }
        if (method != null) {
            message.append(" method=\"").append(method).append('"');
        }
        if (details != null && !details.isEmpty()) {
            message.append(' ').append(details);
        }
        message.append(" thread=\"")
                .append(Thread.currentThread().getName())
                .append('"');
        System.out.println(message);
    }

    /**
     * Add an event listener for DevTools protocol events. Events by default are disabled
     * and need to be enabled on a per domain basis, e.g. by sending Network.enable to enable
     * network related events.
     *
     * @param eventListener the listener to add
     */
    public void addEventListener(EventListener eventListener) {
        eventListeners_.add(eventListener);
    }

    /**
     * Remove an event listener for DevTools protocol events.
     *
     * @param eventListener the listener to remove
     */
    public void removeEventListener(EventListener eventListener) {
        eventListeners_.remove(eventListener);
    }

    public interface EventListener {
        /**
         * Method that will be called on receipt of an event.
         * @param eventName the event name
         * @param messageAsJson JSON object with the event message
         */
        void onEvent(String eventName, String messageAsJson);
    }

    public static final class DevToolsException extends Exception {
        private static final long serialVersionUID = 3952948449841375372L;

        private final String json_;

        public DevToolsException(String message) {
            this(message, null);
        }

        public DevToolsException(String message, String json) {
            super(message);
            this.json_ = json;
        }

        @Override
        public String getMessage() {
            String message = super.getMessage();
            if (json_ != null) message += ": " + json_;
            return message;
        }

        /**
         * JSON object with the error details that were passed back by the DevTools
         * host, or null if no further details are available.
         */
        public String getJson() {
            return json_;
        }
    }
}
