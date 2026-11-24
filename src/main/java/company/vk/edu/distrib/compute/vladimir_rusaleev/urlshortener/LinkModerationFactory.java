package company.vk.edu.distrib.compute.vladimir_rusaleev.urlshortener;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Locale;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationDeadLetterTest;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactory;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactoryTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;

@ModerationServiceFactoryTest
@ModerationDeadLetterTest
public final class LinkModerationFactory implements ModerationServiceFactory {
    @Override
    public ModerationService create(String bootstrapServers) throws IOException {
        return new Worker(bootstrapServers, readWords());
    }

    private static List<String> readWords() throws IOException {
        InputStream input = LinkModerationFactory.class.getResourceAsStream("/moderation/banned_words.txt");
        if (input == null) {
            throw new IOException("Banned words resource is missing");
        }

        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(input, StandardCharsets.UTF_8))) {
            return reader.lines()
                .map(String::strip)
                .filter(word -> !word.isEmpty())
                .map(word -> word.toLowerCase(Locale.ROOT))
                .toList();
        }
    }

    private static final class Worker extends KafkaWorker implements ModerationService {
        private static final String GROUP = "vladimir-rusaleev-moderation";

        private final List<String> words;

        Worker(String bootstrapServers, List<String> words) {
            super(bootstrapServers, GROUP, ModerationEvents.CREATED_TOPIC, true);
            this.words = words;
        }

        @Override
        public void stop() {
            close();
        }

        @Override
        protected void onRecord(
            KafkaProducer<String, byte[]> producer,
            ConsumerRecord<String, byte[]> record
        ) throws IOException {
            ModerationResultEvent result;
            try {
                result = moderate(record);
            } catch (RuntimeException exception) {
                sendDeadLetter(producer, record, exception);
                return;
            }

            send(producer, new ProducerRecord<>(
                ModerationEvents.MODERATED_TOPIC,
                result.id(),
                ModerationEvents.encodeResult(result)
            ));
        }

        private ModerationResultEvent moderate(ConsumerRecord<String, byte[]> record) {
            LinkCreatedEvent event = ModerationEvents.decodeCreated(record.key(), record.value());
            URI uri = URI.create(event.url());
            if (uri.getHost() == null) {
                throw new IllegalArgumentException("Invalid URL");
            }

            String url = URLDecoder.decode(event.url(), StandardCharsets.UTF_8)
                .toLowerCase(Locale.ROOT);
            boolean passed = words.stream().noneMatch(url::contains);

            return new ModerationResultEvent(
                event.id(),
                passed,
                passed ? "" : "URL contains a banned word",
                Instant.now()
            );
        }

        private static void sendDeadLetter(
            KafkaProducer<String, byte[]> producer,
            ConsumerRecord<String, byte[]> record,
            RuntimeException exception
        ) throws IOException {
            RecordHeaders headers = new RecordHeaders(record.headers());
            headers.remove("error");
            headers.add("error", exception.toString().getBytes(StandardCharsets.UTF_8));

            send(producer, new ProducerRecord<>(
                ModerationEvents.DEAD_LETTER_TOPIC,
                null,
                record.timestamp(),
                record.key(),
                record.value(),
                headers
            ));
        }
    }
}
