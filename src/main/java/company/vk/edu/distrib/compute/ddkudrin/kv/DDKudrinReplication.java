package company.vk.edu.distrib.compute.ddkudrin.kv;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;

public class DDKudrinReplication {
    private static final String GET = "GET";
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String INTERNAL_HEADER = "X-DDKudrin-Internal";
    private static final HttpClient CLIENT = HttpClient.newHttpClient();
    private final Dao<byte[]> dao;

    public DDKudrinReplication(Dao<byte[]> dao) {
        this.dao = dao;
    }

    void replicate(HttpExchange exchange, String key, String query, String cluster) throws IOException {
        String[] ports = cluster.split(",");
        Arrays.sort(ports);
        String configuredFactor = System.getProperty("REPLICATION_FACTOR");
        int factor = configuredFactor == null ? ports.length : Integer.parseInt(configuredFactor);
        if (factor < 1 || factor > ports.length) {
            throw new IllegalArgumentException("Invalid replication factor");
        }
        int ack = DDKudrinEntityHandler.readAck(query, factor);
        String method = exchange.getRequestMethod();
        if (!Objects.equals(method, GET) && !Objects.equals(method, "PUT") && !Objects.equals(method, "DELETE")) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }
        byte[] body = Objects.equals(method, "PUT") ? exchange.getRequestBody().readAllBytes() : new byte[0];
        BlockingQueue<ReplicaResponse> responses = new LinkedBlockingQueue<>();
        sendRequests(exchange, key, ports, factor, method, body, responses);
        collectResponses(exchange, method, ack, factor, responses);
    }

    private void sendRequests(HttpExchange exchange, String key, String[] ports, int factor, String method,
                              byte[] body, BlockingQueue<ReplicaResponse> responses) {
        int[] nodePorts = Arrays.stream(ports).mapToInt(port -> Integer.parseInt(port.trim())).toArray();
        int first = new DDKudrinNodeRouter(DDKudrinNodeRouter.Algorithm.CONSISTENT, nodePorts).selectNode(key);
        for (int i = 0; i < factor; i++) {
            String port = ports[(first + i) % ports.length];
            if (Integer.parseInt(port.trim()) == exchange.getLocalAddress().getPort()) {
                responses.add(localResponse(method, key, body));
            } else {
                sendRemote(port, method, key, body, responses);
            }
        }
    }

    private static void sendRemote(String port, String method, String key, byte[] body,
                                   BlockingQueue<ReplicaResponse> responses) {
        String encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8);
        HttpRequest request = HttpRequest.newBuilder(
                URI.create("http://localhost:" + port.trim() + ENTITY_PATH + "?id=" + encodedKey))
                .timeout(Duration.ofSeconds(2))
                .header(INTERNAL_HEADER, "true")
                .method(method, HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
        CLIENT.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
                .whenComplete((response, error) -> responses.add(error == null
                        ? new ReplicaResponse(response.statusCode(), response.body())
                        : new ReplicaResponse(503, new byte[0])));
    }

    private static void collectResponses(HttpExchange exchange, String method, int ack, int factor,
                                         BlockingQueue<ReplicaResponse> responses) throws IOException {
        int successes = 0;
        byte[] value = null;
        for (int remaining = factor; remaining > 0; remaining--) {
            ReplicaResponse response = takeResponse(responses);
            int status = response.status();
            if (isSuccessful(method, status)) {
                successes++;
                if (Objects.equals(method, GET) && status == 200 && value == null) {
                    value = response.body();
                }
            }
            if (successes >= ack) {
                sendResult(exchange, method, value);
                return;
            }
            if (successes + remaining - 1 < ack) {
                break;
            }
        }
        exchange.sendResponseHeaders(503, -1);
    }

    private static ReplicaResponse takeResponse(BlockingQueue<ReplicaResponse> responses) throws IOException {
        try {
            return responses.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for replicas", e);
        }
    }

    private static boolean isSuccessful(String method, int status) {
        return switch (method) {
            case GET -> status == 200 || status == 404;
            case "PUT" -> status == 201;
            case "DELETE" -> status == 202;
            default -> false;
        };
    }

    private static void sendResult(HttpExchange exchange, String method, byte[] value) throws IOException {
        if (Objects.equals(method, GET)) {
            sendGetResult(exchange, value);
        } else {
            exchange.sendResponseHeaders(Objects.equals(method, "PUT") ? 201 : 200, -1);
        }
    }

    private static void sendGetResult(HttpExchange exchange, byte[] value) throws IOException {
        if (value == null) {
            exchange.sendResponseHeaders(404, -1);
        } else {
            exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
            exchange.sendResponseHeaders(200, value.length == 0 ? -1 : value.length);
            if (value.length != 0) {
                exchange.getResponseBody().write(value);
            }
        }
    }

    private ReplicaResponse localResponse(String method, String key, byte[] body) {
        try {
            if (Objects.equals(method, GET)) {
                return new ReplicaResponse(200, dao.get(key));
            }
            if (Objects.equals(method, "PUT")) {
                dao.upsert(key, body);
                return new ReplicaResponse(201, new byte[0]);
            }
            dao.delete(key);
            return new ReplicaResponse(202, new byte[0]);
        } catch (NoSuchElementException e) {
            return new ReplicaResponse(404, new byte[0]);
        } catch (IOException e) {
            return new ReplicaResponse(500, new byte[0]);
        }
    }

    private record ReplicaResponse(int status, byte[] body) { }

}
