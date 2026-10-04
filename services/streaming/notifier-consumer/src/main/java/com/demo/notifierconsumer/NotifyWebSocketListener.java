package com.demo.notifierconsumer;

import org.eclipse.jetty.websocket.api.Session;

/**
 * One instance per connected browser. Browsers never send anything
 * meaningful over this socket (the original Python service ignored
 * incoming frames too); it exists purely to replay the buffer on connect
 * and stay registered for broadcasts until it closes.
 */
public class NotifyWebSocketListener implements Session.Listener.AutoDemanding {

    private final NotifyHub hub;
    private Session session;

    public NotifyWebSocketListener(NotifyHub hub) {
        this.hub = hub;
    }

    @Override
    public void onWebSocketOpen(Session session) {
        this.session = session;
        hub.register(session);
    }

    @Override
    public void onWebSocketText(String message) {
        // Incoming messages are not used.
    }

    @Override
    public void onWebSocketClose(int statusCode, String reason) {
        if (session != null) {
            hub.unregister(session);
        }
    }

    @Override
    public void onWebSocketError(Throwable cause) {
        if (session != null) {
            hub.unregister(session);
        }
    }
}
