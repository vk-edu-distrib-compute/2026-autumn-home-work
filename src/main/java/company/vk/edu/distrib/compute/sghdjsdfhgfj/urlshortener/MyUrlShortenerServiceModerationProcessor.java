package company.vk.edu.distrib.compute.sghdjsdfhgfj.urlshortener;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import company.vk.edu.distrib.compute.sghdjsdfhgfj.moderation.KafkaInitializationUtils;
import company.vk.edu.distrib.compute.sghdjsdfhgfj.moderation.MyModerationService;
import company.vk.edu.distrib.compute.sghdjsdfhgfj.moderation.SerializationUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class MyUrlShortenerServiceModerationProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyUrlShortenerServiceModerationProcessor.class);
    private final MyUrlShortenerService service;

    private final KafkaProducer<String, byte[]> moderationProducer;
    private final KafkaConsumer<String, byte[]> resultConsumer;
    private @Nullable Thread consumerThread;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final ExecutorService moderationSender = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "moderation-sender");
        thread.setDaemon(true);
        return thread;
    });

    public MyUrlShortenerServiceModerationProcessor(MyUrlShortenerService service, String bootstrapServers) {
        this.service = service;
        moderationProducer = KafkaInitializationUtils.newProducer(bootstrapServers);
        resultConsumer = KafkaInitializationUtils.newConsumer(bootstrapServers);
    }

    public void start() {
        resultConsumer.subscribe(List.of(MyModerationService.MODERATED_TOPIC));
        running.set(true);
        consumerThread = new Thread(() -> {
            while (running.get()) {
                ConsumerRecords<String, byte[]> records = resultConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, byte[]> record : records) {
                    processModerationResult(record);
                }
                resultConsumer.commitSync();
            }
        });
        consumerThread.start();
    }

    public void stop() {
        running.set(false);
        if (consumerThread != null) {
            consumerThread.interrupt();
            try {
                consumerThread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        moderationSender.shutdownNow();
        resultConsumer.close();
        moderationProducer.close();
    }

    public void sendEvent(String id, String url) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("sending url {} to kafka", url);
        }
        LinkCreatedEvent event = new LinkCreatedEvent(id, url, Instant.now());
        Optional<byte[]> data = SerializationUtils.serializeLinkCreatedEvent(event);
        data.ifPresent(bytes -> moderationSender.submit(() -> {
            try {
                moderationProducer.send(new ProducerRecord<>(MyModerationService.CREATED_TOPIC, id, bytes));
            } catch (RuntimeException e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("failed to send link {} to kafka: {}", id, e.getMessage());
                }
            }
        }));
    }

    private void processModerationResult(ConsumerRecord<String, byte[]> record) {
        byte[] data = record.value();
        Optional<ModerationResultEvent> event = SerializationUtils.deserializeModerationResultEvent(data);
        if (event.isEmpty()) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("failed to deserialize incoming message");
            }
            return;
        }
        if (event.get().passed()) {
            return;
        }
        try {
            service.deleteUrl(event.get().id());
        } catch (IOException e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }
}
