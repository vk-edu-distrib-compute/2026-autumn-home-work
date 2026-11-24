package company.vk.edu.distrib.compute.artyompeshkov.moderation;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class KafkaSubscription implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(KafkaSubscription.class);
    private static final Duration CLOSE_TIMEOUT = Duration.ofMillis(500);
    private final String bootstrapServers;
    private final String group;
    private final String topic;
    private final Consumer<ConsumerRecord<String, byte[]>> handler;
    private final Thread worker;
    private final AtomicBoolean running = new AtomicBoolean(true);

    KafkaSubscription(String bootstrapServers, String group, String topic,
                      Consumer<ConsumerRecord<String, byte[]>> handler) {
        this.bootstrapServers = bootstrapServers;
        this.group = group;
        this.topic = topic;
        this.handler = handler;
        worker = Thread.ofPlatform().daemon().name(group).unstarted(this::consume);
    }

    void start() {
        worker.start();
    }

    private void consume() {
        try {
            while (running.get()) {
                try {
                    readEvents();
                } catch (RuntimeException e) {
                    if (running.get()) {
                        log.warn("Cannot consume {}, reconnecting", topic, e);
                        Thread.sleep(KafkaUtils.RETRY_DELAY);
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @SuppressWarnings("PMD.UseTryWithResources")
    private void readEvents() {
        KafkaConsumer<String, byte[]> consumer = KafkaUtils.consumer(bootstrapServers, group);
        try {
            consumer.subscribe(List.of(topic));
            while (running.get()) {
                for (ConsumerRecord<String, byte[]> record : consumer.poll(KafkaUtils.POLL_TIMEOUT)) {
                    if (!running.get()) {
                        return;
                    }
                    processRecord(consumer, record);
                }
            }
        } finally {
            boolean interrupted = Thread.interrupted();
            try {
                consumer.close(CLOSE_TIMEOUT);
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void processRecord(KafkaConsumer<String, byte[]> consumer, ConsumerRecord<String, byte[]> record) {
        handler.accept(record);
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        consumer.commitSync(Map.of(partition, new OffsetAndMetadata(record.offset() + 1)));
    }

    @Override
    public void close() {
        running.set(false);
        KafkaUtils.stopWorker(worker);
    }
}
