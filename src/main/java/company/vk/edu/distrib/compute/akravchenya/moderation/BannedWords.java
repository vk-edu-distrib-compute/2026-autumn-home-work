package company.vk.edu.distrib.compute.akravchenya.moderation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

final class BannedWords {

    private static final String RESOURCE = "/moderation/banned_words.txt";

    private final List<String> words;

    BannedWords() {
        this.words = load();
    }

    Optional<String> offenseOf(String url) {
        var normalized = decoded(url).toLowerCase(Locale.ROOT);
        return words.stream()
            .filter(normalized::contains)
            .findFirst();
    }

    private static String decoded(String url) {
        try {
            return URLDecoder.decode(url, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException exception) {
            return url;
        }
    }

    private static List<String> load() {
        try (var stream = BannedWords.class.getResourceAsStream(RESOURCE)) {
            if (stream == null) {
                throw new IllegalStateException("missing resource " + RESOURCE);
            }
            try (var reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                return reader.lines()
                    .map(String::trim)
                    .filter(word -> !word.isEmpty())
                    .toList();
            }
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }
}
