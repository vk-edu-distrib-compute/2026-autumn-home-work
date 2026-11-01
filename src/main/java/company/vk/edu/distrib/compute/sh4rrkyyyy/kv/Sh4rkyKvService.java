package company.vk.edu.distrib.compute.sh4rrkyyyy.kv;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.kv.KVService;
import company.vk.edu.distrib.compute.sh4rrkyyyy.common.ErrorHandler;
import company.vk.edu.distrib.compute.sh4rrkyyyy.common.HttpConstants;
import company.vk.edu.distrib.compute.sh4rrkyyyy.common.HttpStatus;
import company.vk.edu.distrib.compute.sh4rrkyyyy.common.HttpUtils;
import company.vk.edu.distrib.compute.sh4rrkyyyy.common.PersistentDao;
import company.vk.edu.distrib.compute.sh4rrkyyyy.common.Serializers;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Sh4rkyKvService implements KVService {
    HttpServer server;
    PersistentDao<byte[]> dao;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final Sh4rkyReplicator replicator;
    private final int port;

    Sh4rkyKvService(int port) throws IOException {
        this.port = port;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.server.setExecutor(executor);
        this.dao = new PersistentDao<>("kv-" + port, Serializers.BYTES);
        this.replicator = new Sh4rkyReplicator(port, dao, executor);
        server.createContext("/v0/status", new ErrorHandler(this::handleStatus));
        server.createContext(HttpConstants.ENTITY_PATH, new ErrorHandler(this::handleEntity, HttpStatus.BAD_REQUEST));
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(1);
        executor.shutdownNow();
        replicator.close();
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!HttpConstants.GET.equals(exchange.getRequestMethod())) {
            HttpUtils.sendEmptyRsp(exchange, HttpStatus.METHOD_NOT_ALLOWED);
            return;
        }
        HttpUtils.sendEmptyRsp(exchange, HttpStatus.OK);
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        if (!HttpConstants.ENTITY_PATH.equals(path)) {
            HttpUtils.sendEmptyRsp(exchange, HttpStatus.BAD_REQUEST);
            return;
        }
        String query = exchange.getRequestURI().getRawQuery();
        String id = extractParam(query, HttpConstants.ID_PARAM);
        if (id == null || id.isEmpty()) {
            HttpUtils.sendEmptyRsp(exchange, HttpStatus.BAD_REQUEST);
            return;
        }
        int[] ports = getClusterPorts();
        int replicationFactor = getReplicationFactor(ports.length);
        int ack = extractAck(query, replicationFactor);
        int[] replicas = getReplicaPorts(id, ports, replicationFactor);
        String method = exchange.getRequestMethod();
        if (!isSupportedMethod(method)) {
            HttpUtils.sendEmptyRsp(exchange, HttpStatus.METHOD_NOT_ALLOWED);
            return;
        }
        byte[] data = HttpConstants.PUT.equals(method)
                ? exchange.getRequestBody().readAllBytes() : HttpConstants.EMPTY_BODY;
        if (HttpConstants.REPLICA_HEADER_VALUE.equals(
                exchange.getRequestHeaders().getFirst(HttpConstants.REPLICA_HEADER))) {
            sendResponse(exchange, replicator.handleLocal(method, id, data));
        } else {
            sendResponse(exchange, replicator.replicate(method, id, data, replicas, ack));
        }
    }

    private static boolean isSupportedMethod(String method) {
        return HttpConstants.GET.equals(method) || HttpConstants.PUT.equals(method)
                || HttpConstants.DELETE.equals(method);
    }

    private static String extractParam(String query, String name) {
        if (query == null) {
            return null;
        }
        for (String param : query.split("&")) {
            int eq = param.indexOf('=');
            if (eq < 0) {
                if (name.equals(param)) {
                    return "";
                }
                continue;
            }
            if (name.equals(param.substring(0, eq))) {
                return URLDecoder.decode(param.substring(eq + 1), StandardCharsets.UTF_8);
            }
        }
        return null;
    }

    private int[] getClusterPorts() {
        String cluster = System.getProperty("CLUSTER");
        if (cluster == null || cluster.isBlank()) {
            return new int[]{port};
        }
        return Arrays.stream(cluster.split(","))
                .map(String::trim)
                .mapToInt(Integer::parseInt)
                .distinct()
                .sorted()
                .toArray();
    }

    private static int getReplicationFactor(int clusterSize) {
        String factor = System.getProperty("REPLICATION_FACTOR");
        int replicationFactor = factor == null ? clusterSize : Integer.parseInt(factor);
        if (replicationFactor < 1 || replicationFactor > clusterSize) {
            throw new IllegalArgumentException("Invalid replication factor");
        }
        return replicationFactor;
    }

    private static int extractAck(String query, int replicationFactor) {
        String value = extractParam(query, "ack");
        int ack = value == null ? 1 : Integer.parseInt(value);
        if (ack < 1 || ack > replicationFactor) {
            throw new IllegalArgumentException("Invalid ack");
        }
        return ack;
    }

    private static int[] getReplicaPorts(String key, int[] ports, int replicationFactor) {
        int first = Sh4rkyClusterDao.getNodeIndex(key, ports);
        int[] replicas = new int[replicationFactor];
        for (int i = 0; i < replicationFactor; ++i) {
            replicas[i] = ports[(first + i) % ports.length];
        }
        return replicas;
    }

    private static void sendResponse(HttpExchange exchange, Sh4rkyReplicator.ReplicaResponse response)
            throws IOException {
        if (response.statusCode() == HttpStatus.OK) {
            HttpUtils.sendBytes(exchange, HttpStatus.OK, response.body());
        } else {
            HttpUtils.sendEmptyRsp(exchange, response.statusCode());
        }
    }

}
