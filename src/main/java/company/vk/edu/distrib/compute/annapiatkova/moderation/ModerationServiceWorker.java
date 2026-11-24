package company.vk.edu.distrib.compute.annapiatkova.moderation;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.WakeupException;

class ModerationServiceWorker implements Runnable {
    AtomicBoolean closed = new AtomicBoolean(false);
    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;
    private final Set<String> bannedWords;
    private static final String DICTIONARY_PATH = "src/main/resources/moderation/banned_words.txt";
    private static final Locale LOCALE = new Locale("en");
    private static final String GROUP_ID = "moderation-service";

    ModerationServiceWorker(String bootstrapServers) {
        this.producer = KafkaUtils.getProducer(bootstrapServers);
        this.consumer = KafkaUtils.getConsumer(bootstrapServers, GROUP_ID);
        this.bannedWords = new HashSet<>();
        try (
            BufferedReader d = new BufferedReader(
                new InputStreamReader(Files.newInputStream(Paths.get(DICTIONARY_PATH)))
            )
        ) {
            while (true) {
                String bannedWord = d.readLine();
                if (bannedWord == null) {
                    break;
                }
                bannedWords.add(bannedWord.toLowerCase(LOCALE));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void run() {
        try (
            KafkaProducer<String, String> producer = this.producer;
            KafkaConsumer<String, String> consumer = this.consumer
        ) {
            consumer.subscribe(Arrays.asList(KafkaUtils.LINKS_CREATED_TOPIC));
            while (!closed.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    String id = record.key();
                    String link = record.value();
                    producer.send(moderate(id, link));
                }
                consumer.commitSync();
            }
        } catch (WakeupException e) {
            if (!closed.get()) {
                throw e;
            }
        }
    }

    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }

    private ProducerRecord<String, String> moderate(String id, String link) {
        return new ProducerRecord<>(
            KafkaUtils.LINKS_MODERATED_TOPIC,
            id,
            toString(containsBannedWords(link))
        );
    }

    private boolean containsBannedWords(String link) {
        String linkDecoded = URLDecoder.decode(link, StandardCharsets.UTF_8).toLowerCase(LOCALE);
        for (String bannedWord : bannedWords) {
            if (linkDecoded.contains(bannedWord)) {
                return true;
            }
        }
        return false;
    }

    private String toString(Boolean result) {
        if (result) {
            return KafkaUtils.MESSAGE_BAN;
        }
        return KafkaUtils.MESSAGE_PASS;
    }
}
