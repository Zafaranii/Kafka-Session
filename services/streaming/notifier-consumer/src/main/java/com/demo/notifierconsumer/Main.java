package com.demo.notifierconsumer;

import org.eclipse.jetty.server.Server;

public class Main {

    private static final int PORT = 8000;

    public static void main(String[] args) throws Exception {
        String bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:19092,kafka-2:19092,kafka-3:19092");
        String topic = env("NOTIFICATION_TOPIC", "notification-requests");
        // Fixed on purpose - see KafkaConsumerLoop's javadoc.
        String groupId = env("KAFKA_GROUP_ID", "notifier-consumer");

        NotifyHub hub = new NotifyHub();

        KafkaConsumerLoop consumerLoop = new KafkaConsumerLoop(bootstrapServers, topic, groupId, hub);
        Thread consumerThread = new Thread(consumerLoop, "kafka-consumer-loop");
        consumerThread.setDaemon(true);
        consumerThread.start();

        Server server = ServerFactory.build(hub, PORT);
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumerLoop.stop();
            try {
                consumerThread.join(8000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            try {
                server.stop();
            } catch (Exception ignored) {
                // Best-effort during shutdown.
            }
        }, "shutdown-hook"));

        server.join();
    }

    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value == null || value.isEmpty()) ? defaultValue : value;
    }
}
