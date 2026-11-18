package company.vk.edu.distrib.compute.fedorkhokhryakov.urlshortener;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ThreadLocalRandom;

public final class UrlShortenerUtils {
    private static final String ID_ALPHABET =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    private static final int ID_LENGTH = 10;

    private UrlShortenerUtils() {
    }

    public static String generateId() {
        StringBuilder id = new StringBuilder(ID_LENGTH);

        for (int i = 0; i < ID_LENGTH; i++) {
            int index = ThreadLocalRandom.current().nextInt(ID_ALPHABET.length());
            id.append(ID_ALPHABET.charAt(index));
        }

        return id.toString();
    }

    public static boolean isValidId(String id) {
        if (id.length() != ID_LENGTH) {
            return false;
        }

        for (int i = 0; i < id.length(); i++) {
            if (ID_ALPHABET.indexOf(id.charAt(i)) < 0) {
                return false;
            }
        }

        return true;
    }

    public static boolean isValidLink(String link) {
        try {
            URI uri = new URI(link);

            return ("http".equalsIgnoreCase(uri.getScheme())
                || "https".equalsIgnoreCase(uri.getScheme()))
                && uri.getHost() != null;
        } catch (URISyntaxException e) {
            return false;
        }
    }
}
