package company.vk.edu.distrib.compute.virogg.urlshortener.links;

import java.net.IDN;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jspecify.annotations.Nullable;

final class LinkUrlUtils {
    private static final String HTTP_SCHEME = "http";
    private static final String HTTPS_SCHEME = "https";
    private static final Pattern AUTHORITY = Pattern.compile("(?:[^@]*@)?([^:@\\[\\]]+)(?::\\d*)?");

    private LinkUrlUtils() {
    }

    static @Nullable URI toAsciiUri(String link) {
        try {
            URI uri = new URI(link);
            String scheme = uri.getScheme();
            if (!HTTP_SCHEME.equalsIgnoreCase(scheme) && !HTTPS_SCHEME.equalsIgnoreCase(scheme)) {
                return null;
            }
            if (uri.getHost() != null) {
                return uri;
            }
            String authority = uri.getRawAuthority();
            Matcher matcher = AUTHORITY.matcher(authority == null ? "" : authority);
            if (!matcher.matches()) {
                return null;
            }
            int offset = scheme.length() + "://".length();
            int start = offset + matcher.start(1);
            int end = offset + matcher.end(1);
            String asciiHost = IDN.toASCII(link.substring(start, end), IDN.USE_STD3_ASCII_RULES);
            URI ascii = new URI(link.substring(0, start) + asciiHost + link.substring(end));
            return ascii.getHost() == null ? null : ascii;
        } catch (URISyntaxException | IllegalArgumentException e) {
            return null;
        }
    }
}
