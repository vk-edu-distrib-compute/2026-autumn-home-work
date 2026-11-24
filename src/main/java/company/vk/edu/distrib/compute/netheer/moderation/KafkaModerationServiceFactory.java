package company.vk.edu.distrib.compute.netheer.moderation;

import company.vk.edu.distrib.compute.moderation.ModerationDeadLetterTest;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactory;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactoryTest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

@ModerationServiceFactoryTest
@ModerationDeadLetterTest
public final class KafkaModerationServiceFactory implements ModerationServiceFactory {
    @Override
    public ModerationService create(String bootstrapServers) throws IOException {
        var dictionary = KafkaModerationServiceFactory.class.getResourceAsStream("/moderation/banned_words.txt");
        if (dictionary == null) {
            throw new IOException("Missing moderation dictionary");
        }
        try (var reader = new BufferedReader(new InputStreamReader(dictionary, StandardCharsets.UTF_8))) {
            var bannedWords = reader.lines()
                    .map(String::strip)
                    .filter(word -> !word.isEmpty())
                    .map(word -> word.toLowerCase(Locale.ROOT))
                    .toList();
            return new KafkaModerationService(bootstrapServers, bannedWords);
        }
    }
}
