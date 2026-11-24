package company.vk.edu.distrib.compute.flighen.kv;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.http.HttpClient;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class EntityHandler implements HttpHandler {
    private static final Logger log = LoggerFactory.getLogger(EntityHandler.class);
    private static final Pattern PARAMETERS_PATTERN = Pattern.compile(
            "^id=([A-Za-z0-9_-]*)(?:&ack=([^&]*))?$"
    );
    private static final int MIN_ACK = 1;
    private static final byte[] EMPTY_BODY = new byte[0];

    private final int mainPort;
    private final int configuredReplicaFactor;
    private final boolean coordinator;
    private final ReplicationCoordinator replicationCoordinator;

    public EntityHandler(
        Dao<byte[]> dao,
        int mainPort,
        int configuredReplicaFactor,
        HttpClient client,
        boolean coordinator
    ) {
        this.mainPort = mainPort;
        this.configuredReplicaFactor = configuredReplicaFactor;
        this.coordinator = coordinator;
        this.replicationCoordinator = new ReplicationCoordinator(dao, mainPort, client);
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        final var method = exchange.getRequestMethod();

        try (exchange) {
            switch (method) {
                case "GET":
                    handleGet(exchange);
                    break;
                case "PUT":
                    handlePut(exchange);
                    break;
                case "DELETE":
                    handleDelete(exchange);
                    break;
                default:
                    sendEmpty(exchange, HttpStatusCodes.FORBIDDEN);
                    break;
            }
        } catch (IOException e) {
            if (log.isErrorEnabled()) {
                log.error(
                    "I/O error while handling {} {}",
                    exchange.getRequestMethod(),
                    exchange.getRequestURI(),
                    e
                );
            }
            throw e;
        }
    }

    private void handleGet(HttpExchange exchange) throws IOException {
        handleOperation(exchange, "GET", EMPTY_BODY);
    }

    private void handlePut(HttpExchange exchange) throws IOException {
        byte[] value = exchange.getRequestBody().readAllBytes();
        handleOperation(exchange, "PUT", value);
    }

    private void handleDelete(HttpExchange exchange) throws IOException {
        handleOperation(exchange, "DELETE", EMPTY_BODY);
    }

    private void handleOperation(HttpExchange exchange, String method, byte[] value) throws IOException {
        Optional<RequestParameters> parsedParameters = parseParameters(exchange);
        if (parsedParameters.isEmpty()) {
            return;
        }

        RequestParameters parameters = parsedParameters.get();
        if (coordinator) {
            handleCoordinated(exchange, method, parameters, value);
        } else {
            sendResult(exchange, replicationCoordinator.executeLocally(method, parameters.key(), value));
        }
    }

    private void handleCoordinated(
        HttpExchange exchange,
        String method,
        RequestParameters parameters,
        byte[] value
    ) throws IOException {
        final ClusterConfiguration cluster;
        try {
            cluster = ClusterConfiguration.fromProperty(mainPort, configuredReplicaFactor);
        } catch (IllegalArgumentException e) {
            log.error("Invalid cluster configuration", e);
            sendEmpty(exchange, HttpStatusCodes.SERVICE_UNAVAILABLE);
            return;
        }

        if (parameters.ack() > cluster.replicaFactor()) {
            sendEmpty(exchange, HttpStatusCodes.BAD_REQUEST);
            return;
        }

        ReplicationCoordinator.OperationResult result = replicationCoordinator.execute(
            method,
            parameters.key(),
            parameters.ack(),
            value,
            cluster
        );
        sendResult(exchange, result);
    }

    private Optional<RequestParameters> parseParameters(HttpExchange exchange) throws IOException {
        String query = exchange.getRequestURI().getRawQuery();
        if (query == null) {
            sendEmpty(exchange, HttpStatusCodes.UNPROCESSABLE_ENTITY);
            return Optional.empty();
        }

        Matcher matcher = PARAMETERS_PATTERN.matcher(query);
        if (!matcher.matches()) {
            sendEmpty(exchange, HttpStatusCodes.UNPROCESSABLE_ENTITY);
            return Optional.empty();
        }

        String key = matcher.group(1);
        if (key.isEmpty()) {
            sendEmpty(exchange, HttpStatusCodes.BAD_REQUEST);
            return Optional.empty();
        }

        String rawAck = matcher.group(2);
        if (rawAck == null) {
            return Optional.of(new RequestParameters(key, MIN_ACK));
        }
        if (!coordinator) {
            sendEmpty(exchange, HttpStatusCodes.BAD_REQUEST);
            return Optional.empty();
        }

        final int ack;
        try {
            ack = Integer.parseInt(rawAck);
        } catch (NumberFormatException e) {
            sendEmpty(exchange, HttpStatusCodes.BAD_REQUEST);
            return Optional.empty();
        }

        if (ack < MIN_ACK) {
            sendEmpty(exchange, HttpStatusCodes.BAD_REQUEST);
            return Optional.empty();
        }
        return Optional.of(new RequestParameters(key, ack));
    }

    private static void sendResult(
        HttpExchange exchange,
        ReplicationCoordinator.OperationResult result
    ) throws IOException {
        if (result.error() != null) {
            sendEmpty(exchange, HttpStatusCodes.INTERNAL_SERVER_ERROR);
        } else if (result.statusCode() == HttpStatusCodes.OK) {
            exchange.sendResponseHeaders(result.statusCode(), result.body().length);
            exchange.getResponseBody().write(result.body());
        } else {
            sendEmpty(exchange, result.statusCode());
        }
    }

    private static void sendEmpty(HttpExchange exchange, int statusCode) throws IOException {
        exchange.sendResponseHeaders(statusCode, -1);
    }

    private record RequestParameters(String key, int ack) {
    }
}
