package company.vk.edu.distrib.compute.akhunzianov.moderation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Locale;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;

public final class UrlModerator {

    private final List<String> bannedWords;

    public UrlModerator() throws IOException {
        var stream = UrlModerator.class.getResourceAsStream("/moderation/banned_words.txt");
        if (stream == null) {
            throw new IOException("Missing banned words dictionary");
        }
        try (var reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            bannedWords = reader.lines().map(String::strip).filter(word -> !word.isEmpty())
                .map(word -> word.toLowerCase(Locale.ROOT)).toList();
        }
    }

    public ModerationResultEvent check(LinkCreatedEvent event) {
        var url = URLDecoder.decode(event.url(), StandardCharsets.UTF_8).toLowerCase(Locale.ROOT);
        for (var word : bannedWords) {
            if (url.contains(word)) {
                return new ModerationResultEvent(event.id(), false, "Banned word: " + word, Instant.now());
            }
        }
        return new ModerationResultEvent(event.id(), true, "", Instant.now());
    }
}
