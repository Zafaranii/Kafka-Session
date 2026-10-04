package com.demo.notifierconsumer;

import org.eclipse.jetty.websocket.api.Callback;
import org.eclipse.jetty.websocket.api.Session;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Tracks connected WebSocket sessions and a bounded buffer of recently
 * broadcast messages. Restarting this process drops every browser
 * connection (they live here, not in Kafka), and the reconnecting browser
 * races the resuming consumer - replaying the buffer to each newly
 * (re)connected session closes that gap even if the reconnect lands late.
 */
public class NotifyHub {

    private static final int RECENT_BUFFER_SIZE = 100;

    private final CopyOnWriteArrayList<Session> sessions = new CopyOnWriteArrayList<>();
    private final Deque<String> recentMessages = new ArrayDeque<>(RECENT_BUFFER_SIZE);
    private final Object bufferLock = new Object();

    public void register(Session session) {
        synchronized (bufferLock) {
            for (String message : recentMessages) {
                sendQuietly(session, message);
            }
        }
        sessions.add(session);
    }

    public void unregister(Session session) {
        sessions.remove(session);
    }

    public void broadcast(String json) {
        synchronized (bufferLock) {
            if (recentMessages.size() == RECENT_BUFFER_SIZE) {
                recentMessages.removeFirst();
            }
            recentMessages.addLast(json);
        }
        for (Session session : sessions) {
            sendQuietly(session, json);
        }
    }

    private void sendQuietly(Session session, String json) {
        try {
            session.sendText(json, Callback.NOOP);
        } catch (Exception ignored) {
            sessions.remove(session);
        }
    }
}
