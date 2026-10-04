package com.demo.notifierconsumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Polls {@code notification-requests} on a dedicated thread and broadcasts
 * each message to connected WebSocket clients via {@link NotifyHub}.
 *
 * The {@code group.id} is fixed (see Main) on purpose: a stable group means
 * a restart resumes from the last committed offset instead of joining as a
 * fresh group, which is what makes "kill it, restart it, message still
 * arrives" true. Call {@link #stop()} and join this thread before the
 * process exits so {@link KafkaConsumer#close()} runs and sends a clean
 * LeaveGroup - otherwise the broker doesn't notice this member is gone
 * until its session times out, stalling the next instance's reassignment.
 */
public class KafkaConsumerLoop implements Runnable {

    private final String bootstrapServers;
    private final String topic;
    private final String groupId;
    private final NotifyHub hub;
    private final ObjectMapper mapper = new ObjectMapper();
    private final AtomicBoolean stopRequested = new AtomicBoolean(false);

    public KafkaConsumerLoop(String bootstrapServers, String topic, String groupId, NotifyHub hub) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;
        this.hub = hub;
    }

    public void stop() {
        stopRequested.set(true);
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            while (!stopRequested.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    handleRecord(record.value());
                }
            }
        }
    }

    private void handleRecord(String rawValue) {
        try {
            JsonNode envelope = mapper.readTree(rawValue);
            JsonNode payload = envelope.has("payload") ? envelope.get("payload") : envelope;

            ObjectNode outgoing = payload.deepCopy();
            outgoing.put("broadcast_at", System.currentTimeMillis());

            hub.broadcast(mapper.writeValueAsString(outgoing));
        } catch (Exception e) {
            // Malformed message on the topic - skip it rather than taking the
            // consumer loop down.
        }
    }
}
