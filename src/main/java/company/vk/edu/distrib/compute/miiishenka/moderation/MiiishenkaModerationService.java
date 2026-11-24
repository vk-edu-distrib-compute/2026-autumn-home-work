package company.vk.edu.distrib.compute.miiishenka.moderation;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import company.vk.edu.distrib.compute.miiishenka.kafka.KafkaEventConsumer;
import company.vk.edu.distrib.compute.miiishenka.kafka.KafkaEventProducer;
import company.vk.edu.distrib.compute.miiishenka.kafka.Topics;
import company.vk.edu.distrib.compute.miiishenka.serializer.LinkCreatedEventSerializationUtils;
import company.vk.edu.distrib.compute.miiishenka.serializer.ModerationResultEventSerializationUtils;
import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MiiishenkaModerationService implements ModerationService {
    private static final String CONSUMER_GROUP = "miiishenka-moderation";
    private final String bootstrapServers;
    private final List<String> bannedWords;
    private KafkaEventProducer producer;
    private KafkaEventConsumer consumer;
    private State state = State.NEW;

    private enum State {
        NEW,
        STARTED,
        STOPPED
    }

    public MiiishenkaModerationService(String bootstrapServers, List<String> bannedWords) {
        this.bootstrapServers = bootstrapServers;
        this.bannedWords = bannedWords;
    }

    @Override
    public void start() {
        if (state != State.NEW) {
            throw new IllegalStateException();
        }
        producer = new KafkaEventProducer(bootstrapServers);
        consumer = new KafkaEventConsumer(bootstrapServers, Topics.LINKS_CREATED_TOPIC, CONSUMER_GROUP, this::process);
        state = State.STARTED;
    }

    @Override
    public void stop() {
        if (state != State.STARTED) {
            throw new IllegalStateException();
        }
        state = State.STOPPED;
        consumer.close();
        producer.close();
    }

    private void process(ConsumerRecord<String, byte[]> record) {
        try {
            LinkCreatedEvent createdEvent = LinkCreatedEventSerializationUtils.deserialize(record.value());
            String decodedUrl = URLDecoder.decode(createdEvent.url(), StandardCharsets.UTF_8)
                    .toLowerCase(Locale.ROOT);
            Optional<String> bannedWord = bannedWords.stream()
                    .filter(decodedUrl::contains)
                    .findFirst();
            ModerationResultEvent result = new ModerationResultEvent(
                    createdEvent.id(),
                    bannedWord.isEmpty(),
                    bannedWord.orElse(""),
                    Instant.now()
            );
            ProducerRecord<String, byte[]> linkModeratedRecord = new ProducerRecord<>(
                    Topics.LINKS_MODERATED_TOPIC,
                    createdEvent.id(),
                    ModerationResultEventSerializationUtils.serialize(result)
            );
            producer.produce(linkModeratedRecord);
        } catch (IOException | IllegalArgumentException e) {
            sendToDeadLetter(record, e);
        }
    }

    private void sendToDeadLetter(ConsumerRecord<String, byte[]> record, Exception exception) {
        ProducerRecord<String, byte[]> deadLetterRecord = new ProducerRecord<>(
                Topics.LINKS_CREATED_DLT_TOPIC,
                record.key(),
                record.value()
        );
        deadLetterRecord.headers().add("error", exception.toString().getBytes(StandardCharsets.UTF_8));
        producer.produce(deadLetterRecord);
    }
}
