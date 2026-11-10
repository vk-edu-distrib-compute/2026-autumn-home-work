package company.vk.edu.distrib.compute.mperikov.moderation;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public final class KafkaClients {
    public static final String CREATED = "links.created";
    public static final String MODERATED = "links.moderated";
    public static final String DEAD = "links.created.dlt";
    public static final String ERROR = "error";
    public static final String MODERATION_GROUP = "mperikov-moderation";

    private KafkaClients() {
    }

    public static KafkaProducer<String, String> producer(String servers, int blockMillis, int requestMillis) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, blockMillis);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestMillis);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, requestMillis + blockMillis);
        return new KafkaProducer<>(props);
    }

    public static KafkaConsumer<String, String> consumer(String servers, String group) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }
}
