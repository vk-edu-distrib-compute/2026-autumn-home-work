package company.vk.edu.distrib.compute.playingpeano.moderation;

import company.vk.edu.distrib.compute.moderation.ModerationDeadLetterTest;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactory;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactoryTest;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

@ModerationServiceFactoryTest
@ModerationDeadLetterTest
public final class ModerationServiceFactoryImpl implements ModerationServiceFactory {
    private static final String BANNED_WORDS_RESOURCE = "/moderation/banned_words.txt";

    @Override
    public ModerationService create(String bootstrapServers) throws IOException {
        if (bootstrapServers.isBlank()) {
            throw new IllegalArgumentException("Kafka bootstrap servers must not be blank");
        }
        return new KafkaModerationService(bootstrapServers, loadBannedWords());
    }

    private Set<String> loadBannedWords() throws IOException {
        try (InputStream inputStream =
                 ModerationServiceFactoryImpl.class.getResourceAsStream(BANNED_WORDS_RESOURCE)) {
            if (inputStream == null) {
                throw new IOException("Cannot read banned words from " + BANNED_WORDS_RESOURCE);
            }
            return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8)
                .lines()
                .map(word -> word.toLowerCase(Locale.ROOT))
                .filter(word -> !word.isBlank())
                .collect(Collectors.toUnmodifiableSet());
        }
    }
}
