package company.vk.edu.distrib.compute.miiishenka.kafka;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaEventConsumer implements AutoCloseable {
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(200);
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final KafkaConsumer<String, byte[]> consumer;
    private final AtomicBoolean running = new AtomicBoolean();

    public KafkaEventConsumer(
            String bootstrapServers,
            String topicName,
            String consumerGroup,
            Consumer<ConsumerRecord<String, byte[]>> recordConsumer
    ) {
        consumer = createConsumer(bootstrapServers, consumerGroup);
        consumer.subscribe(List.of(topicName));
        running.set(true);
        executor.execute(() -> consume(recordConsumer));
    }

    @Override
    public void close() {
        running.set(false);
        consumer.wakeup();
        executor.close();
    }

    private KafkaConsumer<String, byte[]> createConsumer(String bootstrapServers, String consumerGroup) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(properties);
    }

    private void consume(Consumer<ConsumerRecord<String, byte[]>> recordConsumer) {
        try (consumer) {
            while (running.get()) {
                var records = consumer.poll(POLL_TIMEOUT);
                for (var record : records) {
                    recordConsumer.accept(record);
                }
            }
        } catch (WakeupException e) {
            if (running.get()) {
                throw e;
            }
        }
    }
}
