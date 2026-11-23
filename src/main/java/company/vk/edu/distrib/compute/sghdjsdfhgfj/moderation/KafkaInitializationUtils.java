package company.vk.edu.distrib.compute.sghdjsdfhgfj.moderation;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public final class KafkaInitializationUtils {
    private KafkaInitializationUtils() {
    }

    public static KafkaConsumer<String, byte[]> newConsumer(String bootstrapServers) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstrapServers);
        props.setProperty("group.id", "moderation-service");
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        return new KafkaConsumer<>(props, new StringDeserializer(), new ByteArrayDeserializer());
    }

    public static KafkaProducer<String, byte[]> newProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("linger.ms", 1);
        return new KafkaProducer<>(props, new StringSerializer(), new ByteArraySerializer());
    }
}
