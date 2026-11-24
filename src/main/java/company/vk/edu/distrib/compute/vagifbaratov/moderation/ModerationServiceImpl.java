package company.vk.edu.distrib.compute.vagifbaratov.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;

public final class ModerationServiceImpl extends KafkaWorker implements ModerationService {
    private final List<String> bannedWords;

    public ModerationServiceImpl(String bootstrapServers) throws IOException {
        super(bootstrapServers, "links.created", "vagifbaratov-moderation-v1");

        try (
            InputStream stream = ModerationServiceImpl.class.getResourceAsStream("/moderation/banned_words.txt")
        ) {
            if (stream == null) {
                throw new IOException("Missing banned words dictionary");
            }

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                bannedWords = reader.lines()
                        .map(String::strip)
                        .filter(word -> !word.isEmpty())
                        .map(word -> word.toLowerCase(Locale.ROOT))
                        .toList();
            }
        }
    }

    @Override
    protected void publishPending(Producer<String, String> producer) {
        // Results and dead letters published while processing their input records
    }

    @Override
    protected void process(
        ConsumerRecord<String, String> record,
        Producer<String, String> producer
    ) throws ExecutionException, InterruptedException {
        ModerationResultEvent result;
        try {
            result = moderate(record);
        } catch (RuntimeException exception) {
            forwardToDeadLetter(record, producer, exception);
            return;
        }

        producer.send(new ProducerRecord<>("links.moderated", result.id(), EventCodecUtils.encode(result))).get();
    }

    private ModerationResultEvent moderate(ConsumerRecord<String, String> record) {
        LinkCreatedEvent event = EventCodecUtils.decodeCreated(record.value());

        if (!event.id().equals(record.key())) {
            throw new IllegalArgumentException("Event key does not match link ID");
        }

        String url = URLDecoder.decode(event.url(), StandardCharsets.UTF_8).toLowerCase(Locale.ROOT);
        String bannedWord = bannedWords.stream().filter(url::contains).findFirst().orElse("");
        return new ModerationResultEvent(
            event.id(),
            bannedWord.isEmpty(),
            bannedWord.isEmpty() ? "" : "Banned word: " + bannedWord, Instant.now()
        );
    }

    private void forwardToDeadLetter(
        ConsumerRecord<String, String> record,
        Producer<String, String> producer,
        RuntimeException exception
    ) throws ExecutionException, InterruptedException {
        RecordHeaders headers = new RecordHeaders(record.headers().toArray());
        String error = exception.getClass().getName() + ": " + exception.getMessage();
        headers.remove("error").add("error", error.getBytes(StandardCharsets.UTF_8));
        ProducerRecord<String, String> deadLetter = new ProducerRecord<>(
                "links.created.dlt", null, record.timestamp() < 0 ? null : record.timestamp(),
                record.key(), record.value(), headers);

        producer.send(deadLetter).get();
    }

    @Override
    public void stop() {
        close();
    }
}
