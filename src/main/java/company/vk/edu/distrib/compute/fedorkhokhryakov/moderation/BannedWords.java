package company.vk.edu.distrib.compute.fedorkhokhryakov.moderation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

public final class BannedWords {
    private static final String RESOURCE = "/moderation/banned_words.txt";
    private static final int MAX_DECODE_ROUNDS = 5;

    private final List<String> words;

    public BannedWords(List<String> words) {
        this.words = words.stream()
            .map(String::trim)
            .filter(word -> !word.isEmpty())
            .map(word -> word.toLowerCase(Locale.ROOT))
            .toList();
    }

    public static BannedWords fromResource() throws IOException {
        try (InputStream stream = BannedWords.class.getResourceAsStream(RESOURCE)) {
            if (stream == null) {
                throw new IOException("Resource not found: " + RESOURCE);
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                return new BannedWords(reader.lines().toList());
            }
        }
    }

    public Optional<String> findIn(String url) {
        String current = url;
        for (int round = 0; round < MAX_DECODE_ROUNDS; round++) {
            Optional<String> found = findInText(current);
            if (found.isPresent()) {
                return found;
            }
            String decoded = decode(current);
            if (decoded.equals(current)) {
                break;
            }
            current = decoded;
        }
        return Optional.empty();
    }

    private Optional<String> findInText(String text) {
        String lower = text.toLowerCase(Locale.ROOT);
        return words.stream().filter(lower::contains).findFirst();
    }

    private static String decode(String text) {
        try {
            return URLDecoder.decode(text, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            return text;
        }
    }
}
