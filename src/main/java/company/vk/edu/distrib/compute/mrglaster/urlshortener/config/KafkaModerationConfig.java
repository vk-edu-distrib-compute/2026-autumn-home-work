package company.vk.edu.distrib.compute.mrglaster.urlshortener.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public final class KafkaModerationConfig {

    private static final String GROUP_ID = "ep-kafka-moderation";
    private static final String BANNED_WORDS_RESOURCE = "moderation/banned_words.txt";
    private static final String COMMENT_PREFIX = "#";

    private KafkaModerationConfig() {
    }

    public static Properties getConsumerProperties(String bootstrapServers) {
        requireNonBlank(bootstrapServers, "bootstrapServers");
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

    public static Properties getProducerProperties(String bootstrapServers) {
        requireNonBlank(bootstrapServers, "bootstrapServers");
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    public static Set<String> getBannedWords() {
        return BannedWordsHolder.WORDS;
    }

    private static Set<String> loadBannedWords() {
        try (InputStream stream = KafkaModerationConfig.class.getClassLoader()
                .getResourceAsStream(BANNED_WORDS_RESOURCE)) {
            if (stream == null) {
                throw new IllegalStateException(
                        "Banned words resource not found on classpath: " + BANNED_WORDS_RESOURCE);
            }
            return readBannedWords(stream);
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to load banned words from " + BANNED_WORDS_RESOURCE, e);
        }
    }

    private static Set<String> readBannedWords(InputStream stream) throws IOException {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            return reader.lines()
                    .map(String::trim)
                    .filter(line -> !line.isEmpty() && !line.startsWith(COMMENT_PREFIX))
                    .map(word -> word.toLowerCase(Locale.ROOT))
                    .collect(Collectors.toUnmodifiableSet());
        }
    }

    private static void requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }

    private static final class BannedWordsHolder {
        private static final Set<String> WORDS = loadBannedWords();
    }
}
