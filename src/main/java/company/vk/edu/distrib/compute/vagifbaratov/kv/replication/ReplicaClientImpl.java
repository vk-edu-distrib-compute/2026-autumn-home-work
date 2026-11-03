package company.vk.edu.distrib.compute.vagifbaratov.kv.replication;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.vagifbaratov.kv.ClusterConfig;
import company.vk.edu.distrib.compute.vagifbaratov.kv.KVServiceImpl;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.NoSuchElementException;

import static company.vk.edu.distrib.compute.vagifbaratov.kv.replication.ReplicaUtils.successStatus;

public class ReplicaClientImpl implements ReplicaClient, AutoCloseable {
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(1);
    private static final String REPLICA_HEADER = KVServiceImpl.REPLICA_HEADER;
    private static final String ENTITY_PATH = KVServiceImpl.ENTITY_PATH;
    private static final String ID_ARG = KVServiceImpl.ID_ARG;
    private static final int NOT_FOUND = 404;

    private final int port;
    private final Dao<byte[]> dao;
    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(REQUEST_TIMEOUT).build();

    public ReplicaClientImpl(int port, Dao<byte[]> dao) {
        this.port = port;
        this.dao = dao;
    }

    @Override
    public ReplicaResponse execute(
            int targetPort,
            String method,
            String key,
            byte[] body,
            ClusterConfig cfg
    ) throws IOException, InterruptedException {
        if (targetPort == port) {
            return localQuery(method, key, body);
        }

        String target = cfg.host() + ":" + targetPort;
        return remoteQuery(target, method, key, body);
    }

    private ReplicaResponse remoteQuery(String target, String method, String key, byte[] body)
            throws IOException, InterruptedException {
        String encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8);
        URI uri = URI.create(target + ENTITY_PATH + "?" + ID_ARG + "=" + encodedKey);

        HttpRequest req = HttpRequest.newBuilder(uri).timeout(REQUEST_TIMEOUT)
                .header(REPLICA_HEADER, "true")
                .method(method, HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        HttpResponse<byte[]> resp = client.send(req, HttpResponse.BodyHandlers.ofByteArray());

        return new ReplicaResponse(resp.statusCode(), resp.body());
    }

    public ReplicaResponse localQuery(String method, String key, byte[] body) throws IOException {
        switch (method) {
            case "GET" -> {
                try {
                    return new ReplicaResponse(successStatus(method), dao.get(key));
                } catch (NoSuchElementException e) {
                    return new ReplicaResponse(NOT_FOUND, new byte[0]);
                }
            }
            case "PUT" -> dao.upsert(key, body);
            case "DELETE" -> dao.delete(key);
            default -> throw new IllegalArgumentException("Unsupported method");
        }
        return new ReplicaResponse(successStatus(method), new byte[0]);
    }

    @Override
    public void close() throws IOException {
        client.close();
        dao.close();
    }
}
