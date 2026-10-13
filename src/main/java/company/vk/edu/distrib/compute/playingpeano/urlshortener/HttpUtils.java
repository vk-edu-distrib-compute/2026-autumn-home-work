package company.vk.edu.distrib.compute.playingpeano.urlshortener;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;

import org.jspecify.annotations.Nullable;

final class HttpUtils {
    private static final String TEXT_CONTENT_TYPE = "text/html; charset=utf-8";
    private static final String ID_ALPHABET =
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    private static final int ID_LENGTH = 10;
    private static final int MAX_BODY_SIZE = 65_536;

    private HttpUtils() {
    }

    static String readBody(HttpExchange exchange) throws IOException {
        byte[] bytes = exchange.getRequestBody().readNBytes(MAX_BODY_SIZE + 1);
        if (bytes.length > MAX_BODY_SIZE) {
            throw new IllegalArgumentException("Request body is too large");
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    static boolean hasTextContentType(HttpExchange exchange) {
        @Nullable String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
        return contentType != null && TEXT_CONTENT_TYPE.equalsIgnoreCase(contentType.trim());
    }

    static void sendText(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", TEXT_CONTENT_TYPE);
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
    }

    static void sendEmpty(HttpExchange exchange, int status) throws IOException {
        exchange.sendResponseHeaders(status, -1);
    }

    static void sendUnauthorized(HttpExchange exchange) throws IOException {
        exchange.getResponseHeaders().set("WWW-Authenticate", "Basic realm=\"url-shortener\", charset=\"UTF-8\"");
        sendEmpty(exchange, 401);
    }

    static void sendMethodNotAllowed(HttpExchange exchange, String allowedMethods) throws IOException {
        exchange.getResponseHeaders().set("Allow", allowedMethods);
        sendEmpty(exchange, 405);
    }

    static String randomId(SecureRandom random) {
        StringBuilder result = new StringBuilder(ID_LENGTH);
        for (int index = 0; index < ID_LENGTH; index++) {
            result.append(ID_ALPHABET.charAt(random.nextInt(ID_ALPHABET.length())));
        }
        return result.toString();
    }

    static void validateId(String id) {
        if (id.length() != ID_LENGTH) {
            throw new IllegalArgumentException("ID must contain exactly ten characters");
        }
        for (int index = 0; index < id.length(); index++) {
            if (ID_ALPHABET.indexOf(id.charAt(index)) < 0) {
                throw new IllegalArgumentException("ID must be ASCII alphanumeric");
            }
        }
    }

    static void validateLongLink(String longLink) {
        try {
            URI uri = new URI(longLink);
            boolean supportedScheme = "http".equalsIgnoreCase(uri.getScheme())
                || "https".equalsIgnoreCase(uri.getScheme());
            if (!supportedScheme || uri.getHost() == null || uri.getPort() > 65_535) {
                throw new IllegalArgumentException("Invalid HTTP link");
            }
        } catch (URISyntaxException exception) {
            throw new IllegalArgumentException("Invalid HTTP link", exception);
        }
    }
}
