package company.vk.edu.distrib.compute.vagifbaratov.moderation;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public final class LinkModerationClient extends KafkaWorker {
    private static final Logger log = LoggerFactory.getLogger(LinkModerationClient.class);
    private final BlockingQueue<ProducerRecord<String, String>> pending = new ArrayBlockingQueue<>(1024);
    private final Dao<String> linksDao;

    public LinkModerationClient(String bootstrapServers, Dao<String> linksDao) {
        super(bootstrapServers, "links.moderated", "vagifbaratov-shortener-" + UUID.randomUUID());
        this.linksDao = linksDao;
    }

    public void linkCreated(String id, String url) {
        LinkCreatedEvent event = new LinkCreatedEvent(id, url, Instant.now());
        ProducerRecord<String, String> record = new ProducerRecord<>(
                "links.created", id, EventCodecUtils.encode(event));
        if (!pending.offer(record) && log.isWarnEnabled()) {
            log.warn("Moderation queue full; link {} stay available", id);
        }
    }

    @Override
    protected void publishPending(Producer<String, String> producer) {
        for (int index = 0; index < 32; index++) {
            ProducerRecord<String, String> record = pending.peek();
            if (record == null) {
                return;
            }

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    handleSendFailure(record, exception);
                }
            });

            pending.remove();
        }
    }

    private void handleSendFailure(ProducerRecord<String, String> record, Exception exception) {
        if (log.isWarnEnabled()) {
            log.warn("couldn't publish link {} for moderation", record.key(), exception);
        }
        if (!pending.offer(record) && log.isWarnEnabled()) {
            log.warn("Moderation retry queue is full for link {}", record.key());
        }
    }

    @Override
    protected void process(
        ConsumerRecord<String, String> record,
        Producer<String, String> producer
    ) throws IOException {
        ModerationResultEvent result = EventCodecUtils.decodeModerated(record.value());
        if (!result.id().equals(record.key())) {
            throw new IllegalArgumentException("Result key not match link ID");
        }

        if (!result.passed()) {
            try {
                linksDao.delete(result.id());
            } catch (NoSuchElementException expected) {
                // Repeated delivery or link already deleted by user
            }
        }
    }
}
