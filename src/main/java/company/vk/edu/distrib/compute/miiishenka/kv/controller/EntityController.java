package company.vk.edu.distrib.compute.miiishenka.kv.controller;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Predicate;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.miiishenka.http.BaseController;
import company.vk.edu.distrib.compute.miiishenka.http.exception.BadRequestException;
import company.vk.edu.distrib.compute.miiishenka.http.exception.HttpStatusException;
import company.vk.edu.distrib.compute.miiishenka.http.exception.NotFoundException;
import company.vk.edu.distrib.compute.miiishenka.http.exception.ServiceUnavailableException;
import company.vk.edu.distrib.compute.miiishenka.kv.model.EntityQueryParams;
import company.vk.edu.distrib.compute.miiishenka.kv.model.ReplicaResponse;

public class EntityController extends BaseController {
    private static final String ID_PREFIX = "id=";
    private static final String ACK_PREFIX = "ack=";
    private static final String CLUSTER_PROPERTY = "CLUSTER";
    private static final String REPLICA_HEADER = "X-Replica-Request";
    private final Dao<byte[]> dao;
    private final HttpClient httpClient;
    private final int selfPort;

    public EntityController(Dao<byte[]> dao, int selfPort, HttpClient httpClient) {
        super();
        this.dao = dao;
        this.httpClient = httpClient;
        this.selfPort = selfPort;
    }

    @Override
    public String getPath() {
        return "/v0/entity";
    }

    @Override
    public void get(HttpExchange exchange) throws HttpStatusException, IOException {
        EntityQueryParams params = extractQueryParams(exchange);

        if (isReplicaRequest(exchange) || params.ack() == 1) {
            byte[] value = dao.get(params.id());
            exchange.sendResponseHeaders(200, value.length);
            exchange.getResponseBody().write(value);
            return;
        }

        List<Integer> clusterPorts = getClusterPorts();
        BlockingQueue<ReplicaResponse> replicaResponses = sendReplicaRequests(params.id(), "GET", clusterPorts, null);

        try {
            replicaResponses.add(new ReplicaResponse(200, dao.get(params.id())));
        } catch (NoSuchElementException ignored) {
            replicaResponses.add(new ReplicaResponse(404, null));
        }

        byte[] quorumValue = awaitQuorum(
                clusterPorts.size(),
                params.ack(),
                replicaResponses,
                replicaResponse -> replicaResponse.statusCode() == 200 || replicaResponse.statusCode() == 404,
                replicaResponse -> replicaResponse.statusCode() == 200
        );

        if (quorumValue == null) {
            throw new NotFoundException();
        }

        exchange.sendResponseHeaders(200, quorumValue.length);
        exchange.getResponseBody().write(quorumValue);
    }

    @Override
    public void put(HttpExchange exchange) throws HttpStatusException, IOException {
        EntityQueryParams params = extractQueryParams(exchange);
        byte[] value = exchange.getRequestBody().readAllBytes();
        if (isReplicaRequest(exchange)) {
            dao.upsert(params.id(), value);
            exchange.sendResponseHeaders(201, -1);
            return;
        }

        List<Integer> clusterPorts = getClusterPorts();
        BlockingQueue<ReplicaResponse> replicaResponses = sendReplicaRequests(params.id(), "PUT", clusterPorts, value);

        dao.upsert(params.id(), value);
        replicaResponses.add(new ReplicaResponse(201, null));

        awaitQuorum(
                clusterPorts.size(),
                params.ack(),
                replicaResponses,
                replicaResponse -> replicaResponse.statusCode() == 201,
                ignored -> false
        );

        exchange.sendResponseHeaders(201, -1);
    }

    @Override
    public void delete(HttpExchange exchange) throws HttpStatusException, IOException {
        EntityQueryParams params = extractQueryParams(exchange);
        if (isReplicaRequest(exchange)) {
            dao.delete(params.id());
            exchange.sendResponseHeaders(202, -1);
            return;
        }

        List<Integer> clusterPorts = getClusterPorts();
        BlockingQueue<ReplicaResponse> replicaResponses = sendReplicaRequests(
                params.id(),
                "DELETE",
                clusterPorts,
                null
        );

        dao.delete(params.id());
        replicaResponses.add(new ReplicaResponse(202, null));

        awaitQuorum(
                clusterPorts.size(),
                params.ack(),
                replicaResponses,
                replicaResponse -> replicaResponse.statusCode() == 202,
                ignored -> false
        );

        exchange.sendResponseHeaders(202, -1);
    }

    private List<Integer> getClusterPorts() {
        String cluster = System.getProperty(CLUSTER_PROPERTY);

        if (cluster == null || cluster.isBlank()) {
            return List.of(selfPort);
        }

        return Arrays.stream(cluster.split(","))
                .map(Integer::parseInt)
                .toList();
    }

    private EntityQueryParams extractQueryParams(HttpExchange exchange) throws BadRequestException, NotFoundException {
        validateExactPath(exchange);
        String rawQuery = exchange.getRequestURI().getRawQuery();
        if (rawQuery == null) {
            throw new BadRequestException();
        }
        String[] rawQueryParts = rawQuery.split("&");
        String id = extractParam(rawQueryParts, ID_PREFIX).orElseThrow(BadRequestException::new);
        int ack;
        try {
            ack = extractParam(rawQueryParts, ACK_PREFIX)
                    .map(Integer::parseInt)
                    .orElse(1);
        } catch (NumberFormatException ignored) {
            throw new BadRequestException();
        }
        if (ack < 1 || ack > getClusterPorts().size()) {
            throw new BadRequestException();
        }
        return new EntityQueryParams(id, ack);
    }

    private Optional<String> extractParam(String[] rawQueryParts, String paramPrefix) {
        return Arrays.stream(rawQueryParts)
                .filter(rawQueryPart -> rawQueryPart.startsWith(paramPrefix))
                .map(rawQueryPart -> rawQueryPart.substring(paramPrefix.length()))
                .filter(Predicate.not(String::isBlank))
                .findFirst();
    }

    private boolean isReplicaRequest(HttpExchange exchange) {
        return "true".equals(exchange.getRequestHeaders().getFirst(REPLICA_HEADER));
    }

    private URI getUri(String key, int port) {
        return URI.create("http://localhost:%d/v0/entity?id=%s".formatted(port, key));
    }

    private byte[] awaitQuorum(
            int clusterSize,
            int ack,
            BlockingQueue<ReplicaResponse> replicaResponses,
            Predicate<ReplicaResponse> countsAsAcknowledgement,
            Predicate<ReplicaResponse> containsValue
    ) throws IOException, ServiceUnavailableException {
        int receivedResponses = 0;
        int acknowledgements = 0;
        byte[] value = null;
        while (receivedResponses < clusterSize && acknowledgements < ack) {
            ReplicaResponse replicaResponse;
            try {
                replicaResponse = replicaResponses.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }

            receivedResponses++;
            if (countsAsAcknowledgement.test(replicaResponse)) {
                acknowledgements++;
            }

            if (containsValue.test(replicaResponse)) {
                value = replicaResponse.responseBody();
            }
        }

        if (acknowledgements < ack) {
            throw new ServiceUnavailableException();
        }

        return value;
    }

    private BlockingQueue<ReplicaResponse> sendReplicaRequests(
            String id,
            String method,
            List<Integer> clusterPorts,
            byte[] body
    ) {
        BlockingQueue<ReplicaResponse> replicaResponses = new ArrayBlockingQueue<>(clusterPorts.size());
        HttpRequest.BodyPublisher publisher = body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofByteArray(body);
        for (int port : clusterPorts) {
            if (port == selfPort) {
                continue;
            }

            HttpRequest httpRequest = HttpRequest.newBuilder(getUri(id, port))
                    .header(REPLICA_HEADER, "true")
                    .timeout(Duration.ofSeconds(2))
                    .method(method, publisher)
                    .build();

            httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofByteArray())
                    .whenComplete((response, error) -> {
                        if (error == null) {
                            replicaResponses.add(new ReplicaResponse(response.statusCode(), response.body()));
                        } else {
                            replicaResponses.add(ReplicaResponse.failed());
                        }
                    });
        }

        return replicaResponses;
    }
}
