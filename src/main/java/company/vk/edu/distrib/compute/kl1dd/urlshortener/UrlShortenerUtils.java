package company.vk.edu.distrib.compute.kl1dd.urlshortener;

import java.net.URI;
import java.security.SecureRandom;

final class UrlShortenerUtils {
    private static final String ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final String HTTP_STRING = "http";
    private static final String HTTPS_STRING = "https";

    private static final SecureRandom RANDOM = new SecureRandom();

    private UrlShortenerUtils() {
    }

    static String generateRandomID(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int index = RANDOM.nextInt(ALPHABET.length());
            sb.append(ALPHABET.charAt(index));
        }
        return sb.toString();
    }

    static boolean isValidID(String id) {
        if (id == null || id.length() != 10) {
            return false;
        }

        for (int i = 0; i < 10; i++) {
            if (!isAlphaNumeric(id.charAt(i))) {
                return false;
            }
        }

        return true;
    }

    private static boolean isAlphaNumeric(char current) {
        return ('A' <= current && current <= 'Z')
                || ('a' <= current && current <= 'z')
                || ('0' <= current && current <= '9');
    }

    static boolean isValidURL(String url) {
        try {
            URI tryUri = URI.create(url);
            String connectionType = tryUri.getScheme();
            String site = tryUri.getHost();

            if (connectionType == null || site == null) {
                return false;
            }

            return HTTP_STRING.equals(connectionType) || HTTPS_STRING.equals(connectionType);
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
