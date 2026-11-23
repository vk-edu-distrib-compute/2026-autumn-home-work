package company.vk.edu.distrib.compute.sghdjsdfhgfj.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public class MyModerationService implements ModerationService {
    public static final String CREATED_TOPIC = "links.created";
    public static final String MODERATED_TOPIC = "links.moderated";
    static final String DLT_TOPIC = "links.created.dlt";
    private final Consumer<String, byte[]> consumer;
    private final Producer<String, byte[]> resultProducer;
    private final Producer<String, byte[]> dltProducer;
    private @Nullable Thread consumerThread;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Optional<List<String>> bannedWords;
    private static final Logger LOGGER = LoggerFactory.getLogger(MyModerationService.class);

    public MyModerationService(String bootstrapServers) {
        consumer = KafkaInitializationUtils.newConsumer(bootstrapServers);
        resultProducer = KafkaInitializationUtils.newProducer(bootstrapServers);
        dltProducer = KafkaInitializationUtils.newProducer(bootstrapServers);

        URL filterFileLocation = this.getClass().getClassLoader().getResource("moderation/banned_words.txt");
        Optional<List<String>> bannedWords1;
        if (filterFileLocation != null) {
            try {
                bannedWords1 = Optional.of(Files.readAllLines(Path.of(filterFileLocation.toURI())));
            } catch (URISyntaxException | IOException e) {
                bannedWords1 = Optional.empty();
            }
        } else {
            bannedWords1 = Optional.empty();
        }
        bannedWords = bannedWords1;
    }

    @Override
    public void start() {
        running.set(true);
        consumer.subscribe(List.of(CREATED_TOPIC));
        consumerThread = new Thread(() -> {
            while (running.get()) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("received {} events", records.count());
                }
                for (ConsumerRecord<String, byte[]> record : records) {
                    processRecord(record);
                }
                consumer.commitSync();
            }
        });
        consumerThread.start();
    }

    private void processRecord(ConsumerRecord<String, byte[]> record) {
        byte[] data = record.value();
        Optional<LinkCreatedEvent> event = SerializationUtils.deserializeLinkCreatedEvent(data);
        if (event.isEmpty()) {
            sendDlt(data, "malformed message");
            return;
        }
        if (bannedWords.isEmpty()) {
            sendDlt(data, "failed to parse moderation list");
            return;
        }
        String url;
        try {
            url = URLDecoder.decode(event.get().url(), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            url = event.get().url();
        }
        Optional<String> bannedWord = Optional.empty();
        for (String word : bannedWords.get()) {
            if (url.contains(word)) {
                bannedWord = Optional.of(word);
                break;
            }
        }
        ModerationResultEvent resultEvent = new ModerationResultEvent(
                event.get().id(),
                bannedWord.isEmpty(),
                bannedWord.orElse(""),
                Instant.now()
        );
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("sending result");
        }
        Optional<byte[]> resultData = SerializationUtils.serializeModerationResultEvent(resultEvent);
        resultData.ifPresent(x -> resultProducer.send(new ProducerRecord<>(MODERATED_TOPIC, event.get().id(), x)));
    }

    @Override
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
        consumer.close();
        resultProducer.close();
        dltProducer.close();
    }

    private void sendDlt(byte[] data, String reason) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("sending to dlt due to: {}", reason);
        }
        Header header = new RecordHeader("error", reason.getBytes());
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(DLT_TOPIC, 0, "", data, List.of(header));
        dltProducer.send(record);
    }
}
