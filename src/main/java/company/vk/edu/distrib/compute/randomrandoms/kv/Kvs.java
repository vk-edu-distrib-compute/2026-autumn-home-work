package company.vk.edu.distrib.compute.randomrandoms.kv;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.kv.KVService;
import company.vk.edu.distrib.compute.randomrandoms.util.MethodConstants;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Paths;
import java.rmi.UnexpectedException;
import java.time.Duration;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.stream.Gatherers;
import java.util.stream.Stream;

import static company.vk.edu.distrib.compute.randomrandoms.util.HandlerConstants.codeAndText;
import static company.vk.edu.distrib.compute.randomrandoms.util.HandlerConstants.justCode;

public class Kvs implements KVService {
    private final FileDao dao;

    private final HttpClient client;
    private final HttpServer server;
    private final int port;

    public static final String ENTITY = "/v0/entity";
    public static final String STATUS = "/v0/status";
    public static final String QUERY_ID = "id=";
    public static final String QUERY_ACK = "ack=";
    public static final int INCORRECT_KEY_CODE = 400;
    public static final int NOT_FOUND_CODE = 404;
    public static final int FOUND_CODE = 200;
    public static final int DELETED_CODE = 200;
    public static final int PUT_CODE = 201;
    public static final int UNAVAILABLE_CODE = 503;

    private record Req(String key, int ack) {
    }

    private record GetResp(Optional<byte[]> value, int responded) {
        public GetResp join(GetResp other) {
            if (value.isPresent()) {
                return new GetResp(value, responded + other.responded);
            } else {
                return new GetResp(other.value, responded + other.responded);
            }
        }
    }

    public Kvs(int port) throws IOException {
        String file = String.format("/tmp/kv-%d", port);
        dao = new FileDao(Paths.get(file));
        server = HttpServer.create();
        client = HttpClient.newHttpClient();
        server.setExecutor(Executors.newFixedThreadPool(8));
        server.bind(new InetSocketAddress("localhost", port), 0);
        HttpHandler status = justCode(200);
        server.createContext(STATUS, status);
        server.createContext(ENTITY, this::entity);
        this.port = port;
    }

    public static String path(String key, int port) {
        return String.format("http://localhost:%d%s?%s%s", port, ENTITY, QUERY_ID, key);
    }

    private void entity(HttpExchange exc) throws IOException {
        try {
            Req req;
            try {
                req = parseUri(exc.getRequestURI()).orElseThrow();
            } catch (NoSuchElementException e) {
                justCode(422).handle(exc);
                return;
            }
            if (!reqValid(req)) {
                justCode(INCORRECT_KEY_CODE).handle(exc);
                return;
            }
            if (MethodConstants.DELETE.equals(exc.getRequestMethod())) {
                delete(req, exc);
                return;
            }
            if (MethodConstants.GET.equals(exc.getRequestMethod())) {
                get(req, exc);
                return;
            }
            if (MethodConstants.PUT.equals(exc.getRequestMethod())) {
                var value = exc.getRequestBody().readAllBytes();
                put(req, value, exc);
                return;
            }
            justCode(422).handle(exc);
        } catch (Exception e) {
            justCode(500).handle(exc);
        }
    }

    private boolean reqValid(Req req) {
        return !req.key.isEmpty() && req.ack > 0 && req.ack <= nodesTotal();
    }

    private void get(Req req, HttpExchange exc) throws IOException {
        Optional<byte[]> value;
        try {
            value = Optional.of(dao.get(req.key));
        } catch (NoSuchElementException e) {
            value = Optional.empty();
        }
        var resp = new GetResp(value, 1);
        GetResp finalResp = resp;
        resp = nodes().gather(Gatherers.fold(() -> finalResp, (prev, port) -> {
            if (prev.responded >= req.ack) {
                return prev;
            }
            GetResp next;
            try {
                next = replGet(req.key, port);
            } catch (IOException e) {
                return new GetResp(Optional.empty(), 0);
            }
            return prev.join(next);
        })).findFirst().orElseThrow();
        if (resp.responded < req.ack) {
            justCode(UNAVAILABLE_CODE).handle(exc);
        } else {
            try {
                value = resp.value;
                codeAndText(FOUND_CODE, value.orElseThrow()).handle(exc);
            } catch (NoSuchElementException e) {
                justCode(NOT_FOUND_CODE).handle(exc);
            }
        }
    }

    private GetResp replGet(String key, int port) throws IOException {
        HttpResponse<byte[]> response;
        try {
            response = client
                    .send(
                            HttpRequest.newBuilder().GET().uri(URI.create(path(key, port))).build(),
                            HttpResponse.BodyHandlers.ofByteArray()
                    );
        } catch (ConnectException | InterruptedException e) {
            return new GetResp(Optional.empty(), 0);
        }
        switch (response.statusCode()) {
            case INCORRECT_KEY_CODE -> throw new IllegalArgumentException();
            case NOT_FOUND_CODE -> {
                return new GetResp(Optional.empty(), 1);
            }
            case FOUND_CODE -> {
                return new GetResp(Optional.of(response.body()), 1);
            }
            default -> throw new UnexpectedException("unexpected code");
        }
    }

    private void put(Req req, byte[] value, HttpExchange exc) throws IOException {
        dao.upsert(req.key, value);
        int oks = 1;
        if (req.ack > oks) {
            oks = nodes().map(port -> {
                try {
                    return replPut(req.key, value, port);
                } catch (IOException e) {
                    return 0;
                }
            }).reduce(0, Integer::sum) + 1;
        }
        hanldeModify(exc, req.ack, oks, PUT_CODE);
    }

    private int replPut(String key, byte[] value, int port) throws IOException {
        HttpResponse<Void> response;
        try {
            response = client
                    .send(
                            HttpRequest
                                    .newBuilder()
                                    .PUT(HttpRequest.BodyPublishers.ofByteArray(value))
                                    .uri(URI.create(path(key, port)))
                                    .timeout(Duration.ofMillis(500))
                                    .build(),
                            HttpResponse.BodyHandlers.discarding()
                    );
        } catch (ConnectException | InterruptedException e) {
            return 0;
        }
        if (response.statusCode() == PUT_CODE) {
            return 1;
        }
        throw new UnexpectedException("unexpected code");
    }

    private void delete(Req req, HttpExchange exc) throws IOException {
        dao.delete(req.key);
        int oks = 1;
        if (req.ack > oks) {
            oks = nodes().map(port -> {
                try {
                    return replDelete(req.key, port);
                } catch (IOException e) {
                    return 0;
                }
            }).reduce(0, Integer::sum) + 1;
        }
        hanldeModify(exc, req.ack, oks, DELETED_CODE);
    }

    private void hanldeModify(HttpExchange exc, int expected, int actual, int code) throws IOException {
        if (actual >= expected) {
            justCode(code).handle(exc);
        } else {
            justCode(UNAVAILABLE_CODE).handle(exc);
        }
    }

    private int replDelete(String key, int port) throws IOException {
        HttpResponse<Void> response;
        try {
            response = client
                    .send(
                            HttpRequest
                                    .newBuilder()
                                    .DELETE()
                                    .uri(URI.create(path(key, port)))
                                    .timeout(Duration.ofMillis(500))
                                    .build(),
                            HttpResponse.BodyHandlers.discarding()
                    );
        } catch (ConnectException | InterruptedException e) {
            return 0;
        }
        if (response.statusCode() == DELETED_CODE) {
            return 1;
        }
        throw new UnexpectedException("unexpected code");
    }

    private Optional<Req> parseUri(URI uri) {
        var query = uri.getQuery();
        if (query == null) {
            return Optional.empty();
        }
        String[] parts = query.split("&");
        if (!parts[0].startsWith(QUERY_ID)) {
            return Optional.empty();
        }
        var id = parts[0].substring(QUERY_ID.length());
        var ack = 1;
        if (parts.length > 1 && parts[1].startsWith(QUERY_ACK)) {
            ack = Integer.parseInt(parts[1].substring(QUERY_ACK.length()));
        }
        return Optional.of(new Req(id, ack));
    }

    private int nodesTotal() {
        return (int) nodesRaw().count();
    }

    private Stream<Integer> nodes() {
        return Stream.concat(nodesRaw(), nodesRaw())
                .dropWhile(port -> port != this.port)
                .skip(1)
                .limit(nodesTotal() - 1);
    }

    private Stream<Integer> nodesRaw() {
        return Arrays
                .stream(System.getProperty("CLUSTER").split(","))
                .map(Integer::valueOf);
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(1);
    }
}
