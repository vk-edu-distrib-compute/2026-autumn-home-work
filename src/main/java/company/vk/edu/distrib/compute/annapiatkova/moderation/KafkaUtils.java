package company.vk.edu.distrib.compute.annapiatkova.moderation;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public final class KafkaUtils {
    public static final String LINKS_CREATED_TOPIC = "links.created";
    public static final String LINKS_MODERATED_TOPIC = "links.moderated";
    public static final String MESSAGE_BAN = "BAN";
    public static final String MESSAGE_PASS = "PASS";

    public static KafkaProducer<String, String> getProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(props);
    }

    public static KafkaConsumer<String, String> getConsumer(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", groupId);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("enable.auto.commit", "false");
        props.put("max.poll.records", 1);
        props.put("auto.offset.reset", "earliest");
        return new KafkaConsumer<>(props);
    }

    private KafkaUtils() {
        //
    }
}
