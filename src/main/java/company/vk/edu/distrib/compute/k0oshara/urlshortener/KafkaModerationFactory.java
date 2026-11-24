package company.vk.edu.distrib.compute.k0oshara.urlshortener;

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
public final class KafkaModerationFactory implements ModerationServiceFactory {
    private static final String BANNED_WORDS_RESOURCE = "/moderation/banned_words.txt";

    @Override
    public ModerationService create(String bootstrapServers) throws IOException {
        return new KafkaModerationService(bootstrapServers, loadBannedWords());
    }

    private static List<String> loadBannedWords() throws IOException {
        try (InputStream stream = KafkaModerationFactory.class.getResourceAsStream(BANNED_WORDS_RESOURCE)) {
            if (stream == null) {
                throw new IOException("Missing moderation dictionary: " + BANNED_WORDS_RESOURCE);
            }
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8).lines()
                .map(word -> word.strip().toLowerCase(Locale.ROOT))
                .filter(word -> !word.isEmpty())
                .toList();
        }
    }
}
