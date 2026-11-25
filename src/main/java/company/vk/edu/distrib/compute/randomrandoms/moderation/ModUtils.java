package company.vk.edu.distrib.compute.randomrandoms.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Properties;

public final class ModUtils {
    private static final String GROUP_ID = "my-very-unique-string";
    private static final int PASSED_POS = 1;
    private static final String TRUE_REPR = "1";
    private static final String FALSE_REPR = "0";
    private static final String SEP = "\r\n";

    private ModUtils() {
    }

    public static final class TopicConstants {
        public static final String LINKS_CREATED = "links.created";
        public static final String LINKS_CREATED_DLT = "links.created.dlt";
        public static final String LINKS_MODERATED = "links.moderated";
    }

    public static KafkaConsumer<String, String> getConsumer(String bs, String group) {
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, String.format("%s-%s", GROUP_ID, group));
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bs);
        return new KafkaConsumer<>(props);
    }

    public static KafkaProducer<String, String> getProducer(String bs) {
        Properties props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 150);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bs);
        return new KafkaProducer<>(props);
    }

    public static Optional<ModerationResultEvent> deserializeMre(String s) {
        String[] parts;
        try {
            parts = splitExact(s, 4, "\r\n").orElseThrow();
        } catch (NoSuchElementException e) {
            return Optional.empty();
        }

        boolean ok;
        if (TRUE_REPR.equals(parts[PASSED_POS])) {
            ok = true;
        } else if (FALSE_REPR.equals(parts[PASSED_POS])) {
            ok = false;
        } else {
            return Optional.empty();
        }
        ModerationResultEvent result;
        try {
            result = new ModerationResultEvent(parts[0], ok, parts[2], Instant.parse(parts[3]));
        } catch (DateTimeParseException e) {
            return Optional.empty();
        }
        return Optional.of(result);
    }

    public static Optional<LinkCreatedEvent> deserializeLce(String s) {
        String[] parts;
        try {
            parts = splitExact(s, 3, SEP).orElseThrow();
        } catch (NoSuchElementException e) {
            return Optional.empty();
        }
        LinkCreatedEvent result;
        try {
            result = new LinkCreatedEvent(parts[0], parts[1], Instant.parse(parts[2]));
        } catch (DateTimeParseException e) {
            return Optional.empty();
        }
        return Optional.of(result);
    }

    public static String serializeLce(LinkCreatedEvent lce) {
        StringBuilder sb = new StringBuilder();
        sb
                .append(lce.id())
                .append(SEP)
                .append(lce.url())
                .append(SEP)
                .append(lce.createdAt().toString());
        return sb.toString();
    }

    public static String serializeMre(ModerationResultEvent mre) {
        StringBuilder sb = new StringBuilder();
        sb.append(mre.id()).append(SEP);
        if (mre.passed()) {
            sb.append(TRUE_REPR);
        } else {
            sb.append(FALSE_REPR);
        }
        sb
                .append(SEP)
                .append(mre.reason())
                .append(SEP)
                .append(mre.moderatedAt().toString());
        return sb.toString();
    }

    private static Optional<String[]> splitExact(String s, int parts, String sep) {
        var prts = s.split(sep);
        if (prts.length != parts) {
            return Optional.empty();
        }
        return Optional.of(prts);
    }
}
