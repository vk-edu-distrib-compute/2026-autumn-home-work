package company.vk.edu.distrib.compute.randomrandoms.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class Moderator implements ModerationService {
    private static final String BL_FNAME = "src/main/resources/moderation/banned_words.txt";

    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;
    private final List<String> blacklist;
    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicBoolean fin = new AtomicBoolean(false);
    private Thread thd;

    public Moderator(String bs) throws IOException {
        producer = ModUtils.getProducer(bs);
        consumer = ModUtils.getConsumer(bs, "moderator");
        blacklist = Files.readAllLines(Path.of(BL_FNAME));
    }

    private void loop() {
        while (!fin.get()) {
            ConsumerRecords<String, String> crs;
            crs = consumer.poll(Duration.ofSeconds(1));
            for (var cr : crs) {
                ProducerRecord<String, String> pr;
                try {
                    var lce = ModUtils.deserializeLce(cr.value()).orElseThrow();
                    pr = moderated(moderate(lce));
                } catch (NoSuchElementException e) {
                    pr = dead(cr);
                }
                try {
                    producer.send(pr).get(1, TimeUnit.SECONDS);
                } catch (ExecutionException | TimeoutException e) {
                    throw new IllegalStateException("kafka sdohla", e);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            consumer.commitSync();
        }
        consumer.close();
        producer.close();
    }

    private ModerationResultEvent moderate(LinkCreatedEvent lce) {
        for (String word : blacklist) {
            if (URLDecoder.decode(lce.url(), StandardCharsets.UTF_8).contains(word)) {
                return new ModerationResultEvent(lce.id(), false, "Blacklisted word", Instant.now());
            }
        }
        return new ModerationResultEvent(lce.id(), true, "ok", Instant.now());
    }

    private static ProducerRecord<String, String> dead(ConsumerRecord<String, String> cr) {
        ProducerRecord<String, String> dl = new ProducerRecord<>(
                ModUtils.TopicConstants.LINKS_CREATED_DLT,
                cr.key(),
                cr.value()
        );
        dl.headers().add("error", "incorrect format".getBytes(StandardCharsets.UTF_8));
        return dl;
    }

    private static ProducerRecord<String, String> moderated(ModerationResultEvent mre) {
        return new ProducerRecord<>(ModUtils.TopicConstants.LINKS_MODERATED, mre.id(), ModUtils.serializeMre(mre));
    }

    @Override
    public void start() {
        lock.lock();
        try {
            if (thd != null) {
                throw new IllegalStateException("Double start moderation service");
            }
            consumer.subscribe(Collections.singleton(ModUtils.TopicConstants.LINKS_CREATED));
            fin.set(false);
            thd = Thread.ofVirtual().start(this::loop);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void stop() {
        lock.lock();
        try {
            fin.set(true);
            thd.join(Duration.ofSeconds(1));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            lock.unlock();
        }
    }
}
