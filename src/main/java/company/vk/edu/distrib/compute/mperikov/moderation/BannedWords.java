package company.vk.edu.distrib.compute.mperikov.moderation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

final class BannedWords {
    private final List<String> words;

    private BannedWords(List<String> words) {
        this.words = List.copyOf(words);
    }

    static BannedWords load() throws IOException {
        InputStream input = BannedWords.class.getResourceAsStream("/moderation/banned_words.txt");
        if (input == null) {
            throw new IOException("Banned words are missing");
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
            return new BannedWords(read(reader));
        }
    }

    Optional<String> match(String url) {
        String folded = decode(url).toLowerCase(Locale.ROOT);
        String raw = url.toLowerCase(Locale.ROOT);
        for (String word : words) {
            if (raw.contains(word) || folded.contains(word)) {
                return Optional.of(word);
            }
        }
        return Optional.empty();
    }

    private static List<String> read(BufferedReader reader) throws IOException {
        List<String> found = new ArrayList<>();
        String line = reader.readLine();
        while (line != null) {
            String word = line.trim().toLowerCase(Locale.ROOT);
            if (!word.isEmpty()) {
                found.add(word);
            }
            line = reader.readLine();
        }
        return found;
    }

    private static String decode(String url) {
        try {
            return URLDecoder.decode(url, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException ex) {
            return url;
        }
    }
}
