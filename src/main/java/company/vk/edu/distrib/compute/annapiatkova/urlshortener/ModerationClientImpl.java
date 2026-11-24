package company.vk.edu.distrib.compute.annapiatkova.urlshortener;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.annapiatkova.moderation.KafkaUtils;

class ModerationClientImpl implements ModerationClient {
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Dao<String> dao;
    private final BlockingQueue<ProducerRecord<String, String>> queue = new LinkedBlockingQueue<>();
    private final ProducerWorker producerWorker;
    private final ConsumerWorker consumerWorker;
    private static final String GROUP_ID = "moderation-client";

    ModerationClientImpl(Dao<String> dao, String bootstrapServers) {
        this.dao = dao;
        this.producerWorker = new ProducerWorker(KafkaUtils.getProducer(bootstrapServers));
        this.consumerWorker = new ConsumerWorker(KafkaUtils.getConsumer(bootstrapServers, GROUP_ID));
    }

    @Override
    public void start() {
        Thread t1 = new Thread(producerWorker);
        Thread t2 = new Thread(consumerWorker);
        t1.start();
        t2.start();
    }

    @Override
    public void stop() {
        closed.set(true);
        consumerWorker.wakeup();
    }

    @Override
    public void newLink(String id, String link) {
        try {
            queue.put(new ProducerRecord<>(KafkaUtils.LINKS_CREATED_TOPIC, id, link));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UncheckedInterruptedException("Interrupted while putting records in the buffer", e);
        }
    }

    class ProducerWorker implements Runnable {
        private final KafkaProducer<String, String> producer;

        ProducerWorker(KafkaProducer<String, String> producer) {
            this.producer = producer;
        }

        @Override
        public void run() {
            try (
                KafkaProducer<String, String> producer = this.producer
            ) {
                while (!closed.get()) {
                    try {
                        ProducerRecord<String, String> record = queue.poll(1, TimeUnit.SECONDS);
                        if (record != null) {
                            producer.send(record);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        if (!closed.get()) {
                            throw new UncheckedInterruptedException("Interrupted while waiting for records", e);
                        }
                    }
                }
            }
        }
    }

    class ConsumerWorker implements Runnable {
        private final KafkaConsumer<String, String> consumer;

        ConsumerWorker(KafkaConsumer<String, String> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void run() {
            try (
                KafkaConsumer<String, String> consumer = this.consumer
            ) {
                consumer.subscribe(Arrays.asList(KafkaUtils.LINKS_MODERATED_TOPIC));
                while (!closed.get()) {
                    // At most 1 record because of the consumer configs
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, String> record : records) {
                        String id = record.key();
                        String link = record.value();
                        if (KafkaUtils.MESSAGE_PASS.compareTo(link) == 0) {
                            continue;
                        }
                        try {
                            dao.delete(id);
                            consumer.commitSync();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                }
            } catch (WakeupException e) {
                if (!closed.get()) {
                    throw e;
                }
            }
        }

        public void wakeup() {
            consumer.wakeup();
        }
    }

    class UncheckedInterruptedException extends RuntimeException {
        private static final long serialVersionUID = 10L;

        public UncheckedInterruptedException(String message, Throwable cause) {
            super(message, cause);
        } 
    }
}
