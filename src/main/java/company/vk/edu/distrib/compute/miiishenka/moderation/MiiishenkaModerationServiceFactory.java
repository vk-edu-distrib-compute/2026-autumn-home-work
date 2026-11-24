package company.vk.edu.distrib.compute.miiishenka.moderation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

import company.vk.edu.distrib.compute.moderation.ModerationDeadLetterTest;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactory;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactoryTest;

@ModerationServiceFactoryTest
@ModerationDeadLetterTest
public class MiiishenkaModerationServiceFactory implements ModerationServiceFactory {
    @Override
    public ModerationService create(String bootstrapServers) throws IOException {
        List<String> bannedWords = readBannedWords();
        return new MiiishenkaModerationService(bootstrapServers, bannedWords);
    }

    private List<String> readBannedWords() throws IOException {
        try (InputStream input = getClass().getResourceAsStream("/moderation/banned_words.txt")) {
            if (input == null) {
                throw new IOException("Banned words resource not found");
            }

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
                return reader.lines()
                        .map(String::trim)
                        .filter(word -> !word.isEmpty())
                        .map(word -> word.toLowerCase(Locale.ROOT))
                        .toList();
            }
        }
    }
}
