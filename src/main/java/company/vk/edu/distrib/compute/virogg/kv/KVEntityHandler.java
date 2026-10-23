package company.vk.edu.distrib.compute.virogg.kv;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.virogg.http.HttpUtils;
import org.jspecify.annotations.Nullable;

final class KVEntityHandler implements HttpHandler {
    private final Dao<byte[]> dao;

    KVEntityHandler(Dao<byte[]> dao) {
        this.dao = dao;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!KvApiConstants.ENTITY_PATH.equals(exchange.getRequestURI().getPath())) {
            HttpUtils.sendEmpty(exchange, HttpURLConnection.HTTP_NOT_FOUND);
            return;
        }
        String method = exchange.getRequestMethod();
        if (!HttpUtils.GET.equals(method) && !HttpUtils.PUT.equals(method) && !HttpUtils.DELETE.equals(method)) {
            HttpUtils.sendEmpty(exchange, HttpURLConnection.HTTP_BAD_METHOD);
            return;
        }
        String key;
        try {
            key = parseKey(exchange.getRequestURI().getRawQuery());
        } catch (IllegalArgumentException e) {
            HttpUtils.sendEmpty(exchange, HttpURLConnection.HTTP_BAD_REQUEST);
            return;
        }
        execute(exchange, method, key);
    }

    private void execute(HttpExchange exchange, String method, String key) throws IOException {
        switch (method) {
            case HttpUtils.GET -> get(exchange, key);
            case HttpUtils.PUT -> {
                dao.upsert(key, exchange.getRequestBody().readAllBytes());
                HttpUtils.sendEmpty(exchange, HttpURLConnection.HTTP_CREATED);
            }
            case HttpUtils.DELETE -> {
                dao.delete(key);
                HttpUtils.sendEmpty(exchange, HttpURLConnection.HTTP_ACCEPTED);
            }
            default -> throw new IllegalArgumentException("Unsupported method " + method);
        }
    }

    private void get(HttpExchange exchange, String key) throws IOException {
        byte[] value;
        try {
            value = dao.get(key);
        } catch (NoSuchElementException e) {
            HttpUtils.sendEmpty(exchange, HttpURLConnection.HTTP_NOT_FOUND);
            return;
        }
        exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
        exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, value.length == 0 ? -1 : value.length);
        try (OutputStream response = exchange.getResponseBody()) {
            response.write(value);
        }
    }

    private static String parseKey(@Nullable String query) {
        if (query == null) {
            throw new IllegalArgumentException("Missing id");
        }
        String key = null;
        for (String parameter : query.split("&", -1)) {
            key = parseParameter(parameter, key);
        }
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Missing or empty id");
        }
        return key;
    }

    private static @Nullable String parseParameter(String parameter, @Nullable String key) {
        int separator = parameter.indexOf('=');
        String name = separator < 0 ? parameter : parameter.substring(0, separator);
        if (!KvApiConstants.ID_PARAMETER.equals(decode(name))) {
            return key;
        }
        if (key != null || separator < 0) {
            throw new IllegalArgumentException("Duplicate or missing id");
        }
        return decode(parameter.substring(separator + 1));
    }

    private static String decode(String encoded) {
        String bytes = new String(encoded.getBytes(StandardCharsets.UTF_8), StandardCharsets.ISO_8859_1);
        byte[] decoded = URLDecoder.decode(bytes, StandardCharsets.ISO_8859_1)
                .getBytes(StandardCharsets.ISO_8859_1);
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(decoded)).toString();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("Invalid UTF-8 encoding", e);
        }
    }
}
