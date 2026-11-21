package company.vk.edu.distrib.compute.cloudyy74.urlshortener;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.SecureRandom;

final class Cloudyy74LinkUtils {
    private static final int SHORT_LINK_ID_LENGTH = 10;
    private static final String SHORT_LINK_ID_ALPHABET =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final SecureRandom RANDOM = new SecureRandom();

    private Cloudyy74LinkUtils() {
    }

    static String generateShortLinkId() {
        StringBuilder sb = new StringBuilder(SHORT_LINK_ID_LENGTH);

        for (int i = 0; i < SHORT_LINK_ID_LENGTH; i++) {
            int randomIndex = RANDOM.nextInt(SHORT_LINK_ID_ALPHABET.length());
            sb.append(SHORT_LINK_ID_ALPHABET.charAt(randomIndex));
        }

        return sb.toString();
    }

    static void validateId(String id) {
        if (!id.matches("[A-Za-z0-9]{10}")) {
            throw new IllegalArgumentException("Invalid ID");
        }
    }

    static void validateLink(String link) {
        try {
            URI uri = new URI(link);
            if (!uri.isAbsolute() || uri.getHost() == null) {
                throw new IllegalArgumentException("Invalid link");
            }
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid link", e);
        }
    }
}
