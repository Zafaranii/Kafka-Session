package com.demo.notifierconsumer;

import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.ee10.websocket.server.config.JettyWebSocketServletContainerInitializer;
import org.eclipse.jetty.server.Server;

public final class ServerFactory {

    private ServerFactory() {
    }

    public static Server build(NotifyHub hub, int port) {
        Server server = new Server(port);

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        context.addServlet(new ServletHolder(new HealthServlet()), "/health");

        JettyWebSocketServletContainerInitializer.configure(context, (servletContext, container) ->
                container.addMapping("/ws", (req, resp) -> new NotifyWebSocketListener(hub)));

        server.setHandler(context);
        return server;
    }
}
