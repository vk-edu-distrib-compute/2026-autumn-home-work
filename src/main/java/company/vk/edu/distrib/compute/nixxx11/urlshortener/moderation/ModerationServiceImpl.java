package company.vk.edu.distrib.compute.nixxx11.urlshortener.moderation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import company.vk.edu.distrib.compute.moderation.ModerationService;
import company.vk.edu.distrib.compute.nixxx11.urlshortener.UrlShortenerServiceImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static company.vk.edu.distrib.compute.nixxx11.urlshortener.moderation.ModerationSystem.*;

public class ModerationServiceImpl implements ModerationService {
  private static final Logger LOG = LoggerFactory.getLogger(ModerationServiceImpl.class);

  private static final String GROUP_ID = "moderation-service";
  private static final String BANNED_WORDS_FILE = "/moderation/banned_words.txt";

  private final String bootstrapServers;
  private @Nullable KafkaConsumer<String, String> consumer;
  private @Nullable KafkaProducer<String, String> producer;
  private final ExecutorService consumerExecutor = Executors.newSingleThreadExecutor();

  public ModerationServiceImpl(final String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }

  @Override
  public void start() {
    final List<String> bannedWords;
    try {
      bannedWords = readBannedWords();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    consumer = createConsumer(bootstrapServers, GROUP_ID);
    consumer.subscribe(List.of(CREATED_TOPIC));
    producer = createProducer(bootstrapServers);
    consumerExecutor.submit(() -> runKafkaConsumer(bannedWords));
  }

  private static List<String> readBannedWords() throws IOException {
    try (InputStream input = ModerationServiceImpl.class.getResourceAsStream(BANNED_WORDS_FILE)) {
      if (input == null) {
        throw new IllegalStateException("Banned words file not found");
      }
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {
        return reader.lines().toList();
      }
    }
  }

  private void runKafkaConsumer(final List<String> bannedWords) {
    try {
      while (true) {
        final ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        for (final ConsumerRecord<String, String> record : records) {
          final String key = record.key();
          final String value = record.value();

          final ProducerRecord<String, String> result = processRecord(key, value, bannedWords);
          producer.send(result);
        }
      }
    } catch (final WakeupException e) {
      LOG.info("Finishing consumer loop due to WakeupException");
    } catch (final RuntimeException e) {
      LOG.error("Unexpected exception in consumer loop", e);
    }
  }

  private static ProducerRecord<String, String> processRecord(
      final String key,
      final String value,
      final List<String> bannedWords
  ) {
    try {
      if (!UrlShortenerServiceImpl.isValidLink(value)) {
        return errorRecord(key, value, "Invalid link: " + value);
      }
      final String decodedValue = URLDecoder.decode(value, StandardCharsets.UTF_8);
      for (final String bannedWord : bannedWords) {
        if (decodedValue.contains(bannedWord)) {
          final String message = String.format("Link %s contains banned word %s", value, bannedWord);
          LOG.info("Creating moderation issue for key '{}': {}", key, message);
          return new ProducerRecord<>(MODERATED_TOPIC, key, message);
        }
      }
      LOG.debug("Successful moderation for key '{}'", key);
      return new ProducerRecord<>(MODERATED_TOPIC, key, MODERATED_STATUS);
    } catch (final RuntimeException e) {
      LOG.error("Failed to process record with key '{}'", key, e);
      return errorRecord(key, value, e);
    }
  }

  private static ProducerRecord<String, String> errorRecord(final String key, final String value, final String error) {
    final ProducerRecord<String, String> result = new ProducerRecord<>(DEAD_LETTER_TOPIC, key, value);
    result.headers().add("error", error.getBytes(StandardCharsets.UTF_8));
    return result;
  }

  private static ProducerRecord<String, String> errorRecord(final String key, final String value, final Exception e) {
    return errorRecord(key, value, e.getMessage());
  }

  @Override
  public void stop() {
    if (consumer == null) {
      return;
    }
    consumer.wakeup();
    closeAll(consumerExecutor, consumer, producer);
  }
}
