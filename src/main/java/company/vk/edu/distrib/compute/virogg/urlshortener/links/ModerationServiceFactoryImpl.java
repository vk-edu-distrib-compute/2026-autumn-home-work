package company.vk.edu.distrib.compute.virogg.urlshortener.links;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

import company.vk.edu.distrib.compute.moderation.ModerationDeadLetterTest;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactory;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactoryTest;

@ModerationServiceFactoryTest
@ModerationDeadLetterTest
public final class ModerationServiceFactoryImpl implements ModerationServiceFactory {
    private static final String DICTIONARY = "/moderation/banned_words.txt";

    @Override
    public ModerationService create(String bootstrapServers) throws IOException {
        try (InputStream input = ModerationServiceFactoryImpl.class.getResourceAsStream(DICTIONARY)) {
            if (input == null) {
                throw new IOException("Missing moderation dictionary " + DICTIONARY);
            }
            List<String> bannedWords = new String(input.readAllBytes(), StandardCharsets.UTF_8).lines()
                .map(String::strip)
                .filter(word -> !word.isEmpty())
                .map(word -> word.toLowerCase(Locale.ROOT))
                .toList();
            return new KafkaModerationService(bootstrapServers, bannedWords);
        }
    }
}
