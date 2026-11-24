package company.vk.edu.distrib.compute.sh4rrkyyyy.moderation;

import company.vk.edu.distrib.compute.moderation.ModerationDeadLetterTest;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactory;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactoryTest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

@ModerationServiceFactoryTest
@ModerationDeadLetterTest
public class Sh4rkyModerationServiceFactory implements ModerationServiceFactory {
    private static final String DICTIONARY = "/moderation/banned_words.txt";

    @Override
    public ModerationService create(String bootstrapServers) throws IOException {
        var input = getClass().getResourceAsStream(DICTIONARY);
        if (input == null) {
            throw new IOException("Missing dictionary: " + DICTIONARY);
        }
        try (var reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
            return new Sh4rkyModerationService(bootstrapServers, reader.lines().toList());
        }
    }
}
