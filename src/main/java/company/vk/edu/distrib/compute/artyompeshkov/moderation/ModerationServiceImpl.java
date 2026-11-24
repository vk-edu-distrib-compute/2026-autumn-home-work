package company.vk.edu.distrib.compute.artyompeshkov.moderation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

class ModerationServiceImpl implements ModerationService {
    private static final String ERROR_HEADER = "error";
    private final List<String> bannedWords;
    private final KafkaProducer<String, byte[]> producer;
    private final KafkaSubscription subscription;

    ModerationServiceImpl(String bootstrapServers) throws IOException {
        bannedWords = loadBannedWords();
        producer = KafkaUtils.producer(bootstrapServers);
        subscription = new KafkaSubscription(bootstrapServers, "artyompeshkov-moderation",
                KafkaUtils.CREATED_TOPIC, this::moderate);
    }

    @Override
    public void start() {
        subscription.start();
    }

    @Override
    public void stop() {
        subscription.close();
        producer.close(Duration.ZERO);
    }

    private void moderate(ConsumerRecord<String, byte[]> record) {
        ProducerRecord<String, byte[]> response;
        try {
            LinkCreatedEvent event = EventCodec.decodeCreated(record.value(), record.key());
            ModerationResultEvent result = checkLink(event);
            response = new ProducerRecord<>(KafkaUtils.MODERATED_TOPIC, event.id(), EventCodec.encode(result));
        } catch (RuntimeException e) {
            Headers headers = new RecordHeaders(record.headers().toArray());
            headers.add(ERROR_HEADER, e.toString().getBytes(StandardCharsets.UTF_8));
            response = new ProducerRecord<>(KafkaUtils.DEAD_LETTER_TOPIC, null,
                    record.key(), record.value(), headers);
        }
        try {
            producer.send(response).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CompletionException(e);
        } catch (ExecutionException e) {
            throw new CompletionException(e);
        }
    }

    private ModerationResultEvent checkLink(LinkCreatedEvent event) {
        String url = URLDecoder.decode(event.url().replace("+", "%2B"), StandardCharsets.UTF_8)
                .toLowerCase(Locale.ROOT);
        String reason = bannedWords.stream().filter(url::contains).findFirst()
                .map(word -> "Banned word: " + word).orElse("");
        return new ModerationResultEvent(event.id(), reason.isEmpty(), reason, Instant.now());
    }

    private static List<String> loadBannedWords() throws IOException {
        try (InputStream stream = ModerationServiceImpl.class.getResourceAsStream("/moderation/banned_words.txt")) {
            if (stream == null) {
                throw new IOException("Missing moderation dictionary");
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                return reader.lines().map(String::trim).filter(word -> !word.isEmpty())
                        .map(word -> word.toLowerCase(Locale.ROOT)).toList();
            }
        }
    }
}
