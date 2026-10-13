package company.vk.edu.distrib.compute.nickmish.urlshortener.utils;

import java.net.URI;
import java.net.URISyntaxException;

public final class LinkValidatorUtils {
    private LinkValidatorUtils() {
        throw new UnsupportedOperationException("Utility class");
    }

    public static void validate(String link) {
        if (link.isBlank()) {
            throw new IllegalArgumentException("Link must not be blank");
        }
        URI uri;
        try {
            uri = new URI(link);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid link format", e);
        }
        String scheme = uri.getScheme();
        if (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme)) {
            throw new IllegalArgumentException("Link must use HTTP or HTTPS scheme");
        }
        if (uri.getHost() == null) {
            throw new IllegalArgumentException("Link must have a host");
        }
    }
}
