package company.vk.edu.distrib.compute.nickmish.urlshortener.utils;

import java.security.SecureRandom;

public final class IdGeneratorUtils {
    private static final SecureRandom RANDOM = new SecureRandom();

    private IdGeneratorUtils() {
        throw new UnsupportedOperationException("Utility class");
    }

    public static String generate() {
        StringBuilder sb = new StringBuilder(IdConstants.ID_LENGTH);
        for (int i = 0; i < IdConstants.ID_LENGTH; i++) {
            sb.append(IdConstants.ALPHABET.charAt(RANDOM.nextInt(IdConstants.ALPHABET.length())));
        }
        return sb.toString();
    }
}
