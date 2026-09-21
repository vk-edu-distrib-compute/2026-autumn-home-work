package company.vk.edu.distrib.compute.iizhukov.urlshortener.db;

public final class DataValidationUtils {
    private DataValidationUtils() {

    }

    public static void validateKey(String key) {
        if (key == null
                || key.isEmpty()
                || key.chars().anyMatch(c -> !Character.isLetterOrDigit(c))
        ) {
            throw new IllegalArgumentException("invalid key");
        }
    }

    public static void validateUrl(String url) {
        if (url == null
                || (
                        !url.startsWith("http://")
                        && !url.startsWith("https://")
                )
        ) {
            throw new IllegalArgumentException("invalid url");
        }
    }
}
