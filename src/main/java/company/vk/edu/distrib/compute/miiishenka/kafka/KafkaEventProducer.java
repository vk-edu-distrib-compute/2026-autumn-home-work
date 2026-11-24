package company.vk.edu.distrib.compute.miiishenka.kafka;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaEventProducer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(KafkaEventProducer.class);
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final KafkaProducer<String, byte[]> producer;

    public KafkaEventProducer(String bootstrapServers) {
        producer = createProducer(bootstrapServers);
    }

    public void produce(ProducerRecord<String, byte[]> record) {
        executor.execute(() -> {
            try {
                producer.send(record).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Cannot send to topic", e);
            } catch (ExecutionException e) {
                log.error("Cannot send to topic", e);
            }
        });
    }

    @Override
    public void close() {
        executor.shutdownNow();
        producer.close(Duration.ZERO);
    }

    private KafkaProducer<String, byte[]> createProducer(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(properties);
    }
}
