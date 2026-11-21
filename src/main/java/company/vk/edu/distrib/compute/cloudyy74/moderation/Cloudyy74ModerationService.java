package company.vk.edu.distrib.compute.cloudyy74.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class Cloudyy74ModerationService implements ModerationService {
    private static final Logger log = LoggerFactory.getLogger(Cloudyy74ModerationService.class);
    private static final String GROUP_ID = "cloudyy74-moderation";

    private final List<String> bannedWords;
    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;
    private final Thread worker = new Thread(this::moderateLinks, "cloudyy74-moderation");
    private final AtomicBoolean running = new AtomicBoolean();

    public Cloudyy74ModerationService(String bootstrapServers) throws IOException {
        bannedWords = loadBannedWords();
        producer = Cloudyy74KafkaUtils.createProducer(bootstrapServers);
        consumer = Cloudyy74KafkaUtils.createConsumer(bootstrapServers, GROUP_ID);
    }

    @Override
    public void start() {
        running.set(true);
        worker.start();
    }

    @Override
    public void stop() {
        running.set(false);
        consumer.wakeup();
        try {
            worker.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void moderateLinks() {
        try {
            consumer.subscribe(List.of(Cloudyy74KafkaUtils.CREATED_TOPIC));
            while (running.get()) {
                final var records = consumer.poll(Cloudyy74KafkaUtils.POLL_TIMEOUT);
                for (var record : records) {
                    final ModerationResultEvent result;
                    try {
                        final var event = Cloudyy74KafkaUtils.readCreated(record.value());
                        result = moderate(event);
                    } catch (RuntimeException e) {
                        sendToDeadLetter(record, e);
                        continue;
                    }
                    producer.send(new ProducerRecord<>(Cloudyy74KafkaUtils.MODERATED_TOPIC,
                            result.id(), Cloudyy74KafkaUtils.serialize(result))).get();
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            if (running.get()) {
                log.error("Moderation consumer was interrupted", e);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (KafkaException | ExecutionException e) {
            log.error("Failed to moderate links", e);
        } finally {
            consumer.close(Cloudyy74KafkaUtils.CLOSE_TIMEOUT);
            producer.close(Cloudyy74KafkaUtils.CLOSE_TIMEOUT);
        }
    }

    private void sendToDeadLetter(ConsumerRecord<String, String> record, RuntimeException error)
            throws ExecutionException, InterruptedException {
        final var deadLetter = new ProducerRecord<>(Cloudyy74KafkaUtils.DEAD_LETTER_TOPIC,
                null, record.timestamp(), record.key(), record.value(), record.headers());
        deadLetter.headers().add("error", error.toString().getBytes(StandardCharsets.UTF_8));
        producer.send(deadLetter).get();
    }

    private ModerationResultEvent moderate(LinkCreatedEvent event) {
        final var url = URLDecoder.decode(event.url(), StandardCharsets.UTF_8).toLowerCase(Locale.ROOT);
        for (String word : bannedWords) {
            if (url.contains(word)) {
                return new ModerationResultEvent(event.id(), false, "Banned word: " + word, Instant.now());
            }
        }
        return new ModerationResultEvent(event.id(), true, "", Instant.now());
    }

    private static List<String> loadBannedWords() throws IOException {
        final var stream = Cloudyy74ModerationService.class.getResourceAsStream("/moderation/banned_words.txt");
        if (stream == null) {
            throw new IOException("Banned words dictionary not found");
        }
        try (var reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            return reader.lines()
                    .map(String::trim)
                    .filter(word -> !word.isEmpty())
                    .map(word -> word.toLowerCase(Locale.ROOT))
                    .toList();
        }
    }
}
