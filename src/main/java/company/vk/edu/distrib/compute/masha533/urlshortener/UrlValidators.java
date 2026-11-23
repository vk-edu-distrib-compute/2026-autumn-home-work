package company.vk.edu.distrib.compute.masha533.urlshortener;

import java.net.URI;
import java.net.URISyntaxException;

public final class UrlValidators {
    private UrlValidators() {
    }

    static boolean isValidLink(String link) {
        try {
            URI uri = new URI(link);
            if (uri.getScheme() == null || uri.getHost() == null) {
                return false;
            }
            return "https".equalsIgnoreCase(uri.getScheme()) || "http".equalsIgnoreCase(uri.getScheme());
        } catch (URISyntaxException e) {
            return false;
        }
    }
}
