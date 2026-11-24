package company.vk.edu.distrib.compute.tomovalex.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ModerationService implements company.vk.edu.distrib.compute.moderation.ModerationService {
    private static final Logger log = LoggerFactory.getLogger(ModerationService.class);

    private final KafkaConsumer<String, byte[]> consumer;
    private final KafkaProducer<String, byte[]> producer;
    private final List<String> bannedWords;
    private final Thread worker;
    private final AtomicBoolean running = new AtomicBoolean();

    public ModerationService(String bootstrapServers) throws IOException {
        bannedWords = readBannedWords();
        producer = KafkaUtils.createProducer(bootstrapServers);
        consumer = KafkaUtils.createConsumer(bootstrapServers, "tomovalex-moderation");
        worker = Thread.ofPlatform().name("tomovalex-moderation").unstarted(this::run);
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
        worker.interrupt();
        try {
            worker.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void run() {
        try {
            consumer.subscribe(List.of(KafkaUtils.CREATED_TOPIC));
            while (running.get()) {
                consume();
            }
        } finally {
            KafkaUtils.release(consumer, producer);
        }
    }

    private void consume() {
        try {
            for (ConsumerRecord<String, byte[]> record : consumer.poll(KafkaUtils.POLL_TIMEOUT)) {
                try {
                    producer.send(moderate(record)).get();
                    KafkaUtils.commit(consumer, record);
                } catch (ExecutionException | KafkaException e) {
                    consumer.seek(KafkaUtils.getPartition(record), record.offset());
                    log.warn("Could not save moderation result", e);
                    break;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            running.set(false);
        } catch (KafkaException e) {
            if (running.get()) {
                log.warn("Could not consume links for moderation", e);
            }
        }
    }

    private ProducerRecord<String, byte[]> moderate(ConsumerRecord<String, byte[]> record) {
        try {
            LinkCreatedEvent event = EventUtils.readCreated(record.value());
            if (!Objects.equals(record.key(), event.id())) {
                return deadLetter(record, "Event key does not match link ID");
            }
            String bannedWord = findBannedWord(event.url());
            boolean passed = bannedWord == null;
            String reason = passed ? "" : "Banned word: " + bannedWord;
            ModerationResultEvent result = new ModerationResultEvent(event.id(), passed, reason, Instant.now());
            return new ProducerRecord<>(KafkaUtils.MODERATED_TOPIC, event.id(), EventUtils.serialize(result));
        } catch (IllegalArgumentException | DateTimeException e) {
            return deadLetter(record, e.toString());
        }
    }

    private String findBannedWord(String url) {
        String decodedUrl = URLDecoder.decode(url, StandardCharsets.UTF_8).toLowerCase(Locale.ROOT);
        for (String word : bannedWords) {
            if (decodedUrl.contains(word)) {
                return word;
            }
        }
        return null;
    }

    private static ProducerRecord<String, byte[]> deadLetter(ConsumerRecord<String, byte[]> record, String error) {
        ProducerRecord<String, byte[]> result = new ProducerRecord<>(KafkaUtils.DEAD_LETTER_TOPIC,
            record.key(), record.value());
        record.headers().forEach(header -> result.headers().add(header));
        result.headers().add("error", error.getBytes(StandardCharsets.UTF_8));
        return result;
    }

    private static List<String> readBannedWords() throws IOException {
        var stream = ModerationService.class.getResourceAsStream("/moderation/banned_words.txt");
        if (stream == null) {
            throw new IOException("Banned words dictionary is missing");
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            return reader.lines().map(String::trim).filter(word -> !word.isEmpty())
                .map(word -> word.toLowerCase(Locale.ROOT)).toList();
        }
    }
}
