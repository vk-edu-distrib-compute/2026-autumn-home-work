package company.vk.edu.distrib.compute.sghdjsdfhgfj.kv;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.sghdjsdfhgfj.CustomHttpHandler;
import company.vk.edu.distrib.compute.sghdjsdfhgfj.PersistentDao;
import company.vk.edu.distrib.compute.sghdjsdfhgfj.StatusCodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

class StorageHandler implements CustomHttpHandler {
    private final int port;
    private final PersistentDao storage;
    private Optional<ShardManager> shardManager;
    private static final int DEFAULT_ACK = 1;

    private static final Logger LOG = LoggerFactory.getLogger(StorageHandler.class);

    public StorageHandler(int port, PersistentDao storage) {
        this.port = port;
        this.storage = storage;
        this.shardManager = Optional.empty();
    }

    @Override
    public void handleGet(HttpExchange xch) throws IOException, StatusCodeException {
        parseCluster();
        Map<String, String> params = getQueryParams(xch);
        String id = getId(params);
        int ack = getAck(params);

        if (ack == DEFAULT_ACK) {
            if (storage.containsKey(id)) {
                byte[] value = storage.get(id);
                xch.sendResponseHeaders(200, 0);
                xch.getResponseBody().write(value);
            } else {
                throw StatusCodeException.notFound();
            }
        } else {
            byte[] value = getReplicated(id, ack);
            xch.sendResponseHeaders(200, 0);
            xch.getResponseBody().write(value);
        }
    }

    private byte[] getReplicated(String id, int ack) throws StatusCodeException {
        if (shardManager.isEmpty()) {
            throw StatusCodeException.serviceUnavailable();
        }
        MyRemoteDao[] daos = shardManager.get().determineShards(id);
        List<Optional<byte[]>> results = submitAndWait(daos, ack,
                () -> {
                    if (storage.containsKey(id)) {
                        return Optional.of(storage.get(id));
                    } else {
                        return Optional.empty();
                    }
                },
                (dao) -> {
                    try {
                        return Optional.of(dao.get(id, 1));
                    } catch (NoSuchElementException e) {
                        return Optional.empty();
                    }
                });
        Optional<byte[]> value = results.stream().filter(Optional::isPresent).map(Optional::get).findFirst();
        if (value.isPresent()) {
            return value.get();
        } else {
            throw StatusCodeException.notFound();
        }
    }

    @Override
    public void handlePut(HttpExchange xch) throws IOException, StatusCodeException {
        parseCluster();
        Map<String, String> params = getQueryParams(xch);
        String id = getId(params);
        int ack = getAck(params);
        byte[] value = xch.getRequestBody().readAllBytes();

        if (ack == DEFAULT_ACK) {
            storage.upsert(id, value);
            xch.sendResponseHeaders(201, 0);
        } else {
            if (shardManager.isEmpty()) {
                throw StatusCodeException.serviceUnavailable();
            }
            MyRemoteDao[] daos = shardManager.get().determineShards(id);
            submitAndWait(daos, ack, () -> {
                storage.upsert(id, value);
                return true;
            }, (dao) -> {
                dao.upsert(id, value, 1);
                return true;
            });
            xch.sendResponseHeaders(201, 0);
        }
    }

    @Override
    public void handleDelete(HttpExchange xch) throws IOException, StatusCodeException {
        parseCluster();
        Map<String, String> params = getQueryParams(xch);
        String id = getId(params);
        int ack = getAck(params);

        if (ack == DEFAULT_ACK) {
            storage.delete(id);
            xch.sendResponseHeaders(202, 0);
        } else {
            if (shardManager.isEmpty()) {
                throw StatusCodeException.serviceUnavailable();
            }
            MyRemoteDao[] daos = shardManager.get().determineShards(id);
            submitAndWait(daos, ack, () -> {
                storage.delete(id);
                return true;
            }, (dao) -> {
                dao.delete(id);
                return true;
            });
            xch.sendResponseHeaders(202, 0);
        }
    }

    private <T> List<T> submitAndWait(
            MyRemoteDao[] daos,
            int ack,
            Callable<T> self,
            MyRemoteDaoFunction<T> other
    ) throws StatusCodeException {
        int jobCount = daos.length;

        ExecutorService executor = Executors.newFixedThreadPool(jobCount);

        try (executor) {
            CompletionService<T> completionService = new ExecutorCompletionService<>(executor);
            for (MyRemoteDao dao : daos) {
                if (dao.getPort() == this.port) {
                    completionService.submit(self);
                } else {
                    completionService.submit(() -> other.apply(dao));
                }
            }

            List<T> results = new ArrayList<>();

            for (int i = 0; i < jobCount; i++) {
                try {
                    Future<T> resultFuture = completionService.take();
                    T result = resultFuture.get();
                    results.add(result);
                    if (results.size() >= ack) {
                        return results;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    if (LOG.isWarnEnabled()) {
                        LOG.warn("request failed", e.getCause());
                    }
                }
            }
            throw StatusCodeException.serviceUnavailable();
        }
    }

    private Map<String, String> getQueryParams(HttpExchange xch) {
        // pmd is forcing me to use ConcurrentHashMap for a local variable for some reason
        Map<String, String> queryPairs = new ConcurrentHashMap<>();
        String query = xch.getRequestURI().getQuery();
        if (query == null) {
            return queryPairs;
        }
        String[] pairs = query.split("&");
        for (String pair : pairs) {
            int idx = pair.indexOf('=');
            queryPairs.put(
                    URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8),
                    URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8)
            );
        }
        return queryPairs;
    }

    private String getId(Map<String, String> params) throws StatusCodeException {
        if (!params.containsKey("id")) {
            throw StatusCodeException.badRequest();
        }
        String id = params.get("id");
        if (id.isEmpty()) {
            throw StatusCodeException.badRequest();
        }
        return id;
    }

    private int getAck(Map<String, String> params) throws StatusCodeException {
        if (!params.containsKey("ack")) {
            return 1;
        }
        int ack;
        try {
            ack = Integer.parseInt(params.get("ack"));
        } catch (NumberFormatException e) {
            throw StatusCodeException.badRequest(e);
        }
        if (ack <= 0 || (shardManager.isPresent() && ack > shardManager.get().getShardCount())) {
            throw StatusCodeException.badRequest();
        }
        return ack;
    }

    private void parseCluster() {
        if (shardManager.isEmpty()) {
            String clusterDescription = System.getProperty("CLUSTER");
            if (clusterDescription == null) {
                return;
            }
            List<Integer> ports = Arrays.stream(clusterDescription.split(",")).map(Integer::parseInt).toList();
            int[] portsArray = new int[ports.size()];
            for (int i = 0; i < ports.size(); i++) {
                portsArray[i] = ports.get(i);
            }
            ShardManager sm = new ShardManager(portsArray);
            sm.setReplicationFactor(sm.getShardCount());
            shardManager = Optional.of(sm);
        }
    }
}
