package company.vk.edu.distrib.compute.rsmt98.moderation;

import company.vk.edu.distrib.compute.moderation.ModerationDeadLetterTest;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactory;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactoryTest;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

@ModerationServiceFactoryTest
@ModerationDeadLetterTest
public final class KafkaModerationServiceFactory implements ModerationServiceFactory {
    @Override
    public ModerationService create(String bootstrapServers) throws IOException {
        try (InputStream input =
                KafkaModerationServiceFactory.class.getResourceAsStream(
                        "/moderation/banned_words.txt")) {
            if (input == null) {
                throw new IOException("Banned words resource is unavailable");
            }
            List<String> words =
                    new String(input.readAllBytes(), StandardCharsets.UTF_8)
                            .lines()
                            .filter(word -> !word.isEmpty())
                            .map(word -> word.toLowerCase(Locale.ROOT))
                            .toList();
            return new KafkaModerationService(bootstrapServers, words);
        }
    }
}
