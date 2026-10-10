package company.vk.edu.distrib.compute.artyompeshkov.urlshortener;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;

import com.sun.net.httpserver.HttpExchange;

final class HttpUtils {
    static final long NO_BODY = -1;

    private HttpUtils() {
    }

    static void sendText(HttpExchange exchange, int statusCode, String text) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
        byte[] body = text.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(statusCode, body.length);
        exchange.getResponseBody().write(body);
    }
    
    static void sendMethodNotAllowed(HttpExchange exchange, String allowedMethods) throws IOException {
        exchange.getResponseHeaders().set("Allow", allowedMethods);
        exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, NO_BODY);
    }

    static void sendRedirect(HttpExchange exchange, String location) throws IOException {
        exchange.getResponseHeaders().set("Location", location);
        exchange.sendResponseHeaders(HttpURLConnection.HTTP_MOVED_PERM, NO_BODY);
    }

    static void sendUnauthorized(HttpExchange exchange) throws IOException {
        exchange.getResponseHeaders().set("WWW-Authenticate", "Basic realm=\"shortener\", charset=\"UTF-8\"");
        exchange.sendResponseHeaders(HttpURLConnection.HTTP_UNAUTHORIZED, NO_BODY);
    }
}
