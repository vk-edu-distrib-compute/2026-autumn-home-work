package company.vk.edu.distrib.compute.sh4rrkyyyy.kv;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.kv.KVService;
import company.vk.edu.distrib.compute.sh4rrkyyyy.common.ErrorHandler;
import company.vk.edu.distrib.compute.sh4rrkyyyy.common.HttpUtils;
import company.vk.edu.distrib.compute.sh4rrkyyyy.common.PersistentDao;
import company.vk.edu.distrib.compute.sh4rrkyyyy.common.Serializers;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

public class Sh4rkyKvService implements KVService {
    HttpServer server;
    PersistentDao<byte[]> dao;

    private static final String GET = "GET";
    private static final String PUT = "PUT";
    private static final String DELETE = "DELETE";
    private static final String ENTITY_PATH = "/v0/entity";

    Sh4rkyKvService(int port) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.server.setExecutor(Executors.newFixedThreadPool(1));
        this.dao = new PersistentDao<>("kv", Serializers.BYTES);
        server.createContext("/v0/status", new ErrorHandler(this::handleStatus));
        server.createContext(ENTITY_PATH, new ErrorHandler(this::handleEntity));
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(1);
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!GET.equals(exchange.getRequestMethod())) {
            HttpUtils.sendEmptyRsp(exchange, 405);
            return;
        }
        HttpUtils.sendEmptyRsp(exchange, 200);
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        if (!ENTITY_PATH.equals(path)) {
            HttpUtils.sendEmptyRsp(exchange, 400);
            return;
        }
        String id = extractId(exchange.getRequestURI().getQuery());
        if (id == null || id.isEmpty()) {
            HttpUtils.sendEmptyRsp(exchange, 400);
            return;
        }
        switch (exchange.getRequestMethod()) {
            case GET -> handleGet(exchange, id);
            case PUT -> handlePut(exchange, id);
            case DELETE -> handleDelete(exchange, id);
            default -> HttpUtils.sendEmptyRsp(exchange, 405);

        }
    }

    private static String extractId(String query) {
        if (query == null) {
            return null;
        }
        for (String param : query.split("&")) {
            int eq = param.indexOf('=');
            if (eq < 0) {
                continue;
            }
            if ("id".equals(param.substring(0, eq))) {
                return URLDecoder.decode(param.substring(eq + 1), StandardCharsets.UTF_8);
            }
        }
        return null;
    }

    private void handleDelete(HttpExchange exchange, String key) throws IOException {
        dao.delete(key);
        HttpUtils.sendEmptyRsp(exchange, 202);
    }

    private void handlePut(HttpExchange exchange, String key) throws IOException {
        byte[] data = exchange.getRequestBody().readAllBytes();
        dao.upsert(key, data);
        HttpUtils.sendEmptyRsp(exchange, 201);
    }

    private void handleGet(HttpExchange exchange, String key) throws IOException {
        HttpUtils.sendBytes(exchange, 200, dao.get(key));
    }

}
