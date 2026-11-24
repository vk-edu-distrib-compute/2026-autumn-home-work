package company.vk.edu.distrib.compute.playingpeano.kv;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;

final class HttpResponses {
    static final int STATUS_OK = 200;
    static final int STATUS_CREATED = 201;
    static final int STATUS_ACCEPTED = 202;
    static final int STATUS_BAD_REQUEST = 400;
    static final int STATUS_NOT_FOUND = 404;
    static final int STATUS_METHOD_NOT_ALLOWED = 405;
    static final int STATUS_SERVER_ERROR = 500;
    static final int STATUS_UNAVAILABLE = 503;

    private static final String ALLOW_HEADER = "Allow";

    private HttpResponses() {
    }

    static void sendValue(HttpExchange exchange, byte[] value) throws IOException {
        exchange.sendResponseHeaders(STATUS_OK, value.length == 0 ? -1 : value.length);
        if (value.length > 0) {
            exchange.getResponseBody().write(value);
        }
    }

    static void sendEmpty(HttpExchange exchange, int status) throws IOException {
        exchange.sendResponseHeaders(status, -1);
    }

    static void sendMethodNotAllowed(HttpExchange exchange, String allowedMethods) throws IOException {
        exchange.getResponseHeaders().set(ALLOW_HEADER, allowedMethods);
        sendEmpty(exchange, STATUS_METHOD_NOT_ALLOWED);
    }
}
