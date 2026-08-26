package company.vk.edu.distrib.compute.test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.ThreadLocalRandom;

public enum TestUtils {
    ;

    public static final Duration TIMEOUT = Duration.ofSeconds(5);
    public static final String TEST_LINK_ID = "10db3750xYz";
    public static final String TEST_LONG_LINK = "https://ya.ru/search/?text=test";
    public static final String TEST_LONG_LINK_2 = "https://ya.ru/search/?text=test2";
    public static final String CONTENT_TYPE_TEXT = "text/html; charset=utf-8";
    public static final Credentials TEST_CREDENTIALS = new Credentials("test-user", "super_pass");

    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(2);
    private static final String LINKS_PATH = "/v0/links/";

    public static int randomPort() {
        for (int j = 0; j < 5; j++) {
            for (int i = 0; i < 100_000; i++) {
                final var port = ThreadLocalRandom.current().nextInt(10000, 60000);
                if (isTcpPortAvailable(port)) {
                    return port;
                }
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new IllegalStateException("Interrupted while looking for available port");
            }
        }
        throw new IllegalStateException("Can't find available port");
    }

    public static boolean isTcpPortAvailable(int port) {
        try (ServerSocket serverSocket = new ServerSocket()) {
            serverSocket.setReuseAddress(false);
            serverSocket.bind(new InetSocketAddress(InetAddress.getByName("localhost"), port), 1);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    public static int status(HttpClient httpClient, int port) throws IOException, URISyntaxException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
            .GET()
            .uri(new URI("http://localhost:" + port + "/v0/status"))
            .timeout(REQUEST_TIMEOUT)
            .build();
        HttpResponse<Void> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
        return response.statusCode();
    }

    public static HttpResponse<String> create(HttpClient httpClient, int port, String longLink)
        throws IOException, InterruptedException, URISyntaxException {
        return create(httpClient, port, longLink, null);
    }

    public static HttpResponse<String> create(HttpClient httpClient, int port, String longLink, Credentials credentials)
        throws IOException, InterruptedException, URISyntaxException {
        HttpRequest.Builder request = HttpRequest.newBuilder()
            .POST(HttpRequest.BodyPublishers.ofString(longLink))
            .uri(new URI("http://localhost:" + port + "/v0/links"))
            .header("Content-Type", CONTENT_TYPE_TEXT)
            .timeout(REQUEST_TIMEOUT);
        withAuthorization(request, credentials);
        return httpClient.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    public static HttpResponse<String> get(HttpClient httpClient, int port, String id)
        throws IOException, InterruptedException, URISyntaxException {
        return get(httpClient, port, id, null);
    }

    public static HttpResponse<String> get(HttpClient httpClient, int port, String id, Credentials credentials)
        throws IOException, InterruptedException, URISyntaxException {
        HttpRequest.Builder request = HttpRequest.newBuilder()
            .GET()
            .uri(new URI("http://localhost:%d%s%s".formatted(port, LINKS_PATH, id)))
            .timeout(REQUEST_TIMEOUT);
        withAuthorization(request, credentials);
        return httpClient.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    public static HttpResponse<Void> update(HttpClient httpClient, int port, String id, String longLink)
        throws IOException, InterruptedException, URISyntaxException {
        return update(httpClient, port, id, longLink, null);
    }

    public static HttpResponse<Void> update(HttpClient httpClient, int port, String id, String longLink, Credentials credentials)
        throws IOException, InterruptedException, URISyntaxException {
        HttpRequest.Builder request = HttpRequest.newBuilder()
            .PUT(HttpRequest.BodyPublishers.ofString(longLink))
            .uri(new URI("http://localhost:%d%s%s".formatted(port, LINKS_PATH, id)))
            .header("Content-Type", CONTENT_TYPE_TEXT)
            .timeout(REQUEST_TIMEOUT);
        withAuthorization(request, credentials);
        return httpClient.send(request.build(), HttpResponse.BodyHandlers.discarding());
    }

    public static HttpResponse<Void> delete(HttpClient httpClient, int port, String id)
        throws IOException, InterruptedException, URISyntaxException {
        return delete(httpClient, port, id, null);
    }

    public static HttpResponse<Void> delete(HttpClient httpClient, int port, String id, Credentials credentials)
        throws IOException, InterruptedException, URISyntaxException {
        HttpRequest.Builder request = HttpRequest.newBuilder()
            .DELETE()
            .uri(new URI("http://localhost:%d%s%s".formatted(port, LINKS_PATH, id)))
            .timeout(REQUEST_TIMEOUT);
        withAuthorization(request, credentials);
        return httpClient.send(request.build(), HttpResponse.BodyHandlers.discarding());
    }

    public static String extractId(int port, String shortLink) throws URISyntaxException {
        URI uri = new URI(shortLink);
        if (uri.getHost() == null || uri.getPort() != port || uri.getPath() == null) {
            throw new IllegalArgumentException("Unexpected short link: " + shortLink);
        }

        String id = uri.getPath().replaceFirst("/", "");
        if (id.isBlank()) {
            throw new IllegalArgumentException("Short link id is blank");
        }

        return id;
    }

    public static String header(HttpResponse<?> response, String name) {
        return response.headers().firstValue(name).orElseThrow();
    }

    private static void withAuthorization(HttpRequest.Builder request, Credentials credentials) {
        if (credentials != null) {
            String token = Base64.getEncoder()
                .encodeToString((credentials.username() + ":" + credentials.password()).getBytes(StandardCharsets.UTF_8));
            request.header("Authorization", "Basic " + token);
        }
    }

    public record Credentials(String username, String password) {
    }
}
