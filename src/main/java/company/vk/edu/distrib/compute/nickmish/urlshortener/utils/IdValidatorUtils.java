package company.vk.edu.distrib.compute.nickmish.urlshortener.utils;

public final class IdValidatorUtils {
    private static final int ID_LENGTH = 10;
    private static final String ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    private IdValidatorUtils() {
        throw new UnsupportedOperationException("Utility class");
    }

    public static void validate(String id) {
        if (id.length() != ID_LENGTH) {
            throw new IllegalArgumentException("ID must be " + ID_LENGTH + " characters long");
        }
        for (int i = 0; i < id.length(); i++) {
            if (ALPHABET.indexOf(id.charAt(i)) < 0) {
                throw new IllegalArgumentException("ID must contain only alphanumeric characters");
            }
        }
    }
}
