package company.vk.edu.distrib.compute.nickmish.urlshortener.utils;

public final class IdValidatorUtils {
    private IdValidatorUtils() {
        throw new UnsupportedOperationException("Utility class");
    }

    public static void validate(String id) {
        if (id.length() != IdConstants.ID_LENGTH) {
            throw new IllegalArgumentException("ID must be " + IdConstants.ID_LENGTH + " characters long");
        }
        for (int i = 0; i < id.length(); i++) {
            if (IdConstants.ALPHABET.indexOf(id.charAt(i)) < 0) {
                throw new IllegalArgumentException("ID must contain only alphanumeric characters");
            }
        }
    }
}
