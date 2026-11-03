package company.vk.edu.distrib.compute.mrglaster.kv.replication.replica;

import company.vk.edu.distrib.compute.Dao;
import java.io.IOException;
import java.util.NoSuchElementException;

public final class LocalReplicaExecutor {

    private static final byte[] EMPTY = new byte[0];

    private final Dao<byte[]> localDao;

    public LocalReplicaExecutor(Dao<byte[]> localDao) {
        this.localDao = localDao;
    }

    public ReplicaResult execute(String method, String key, byte[] body) {
        try {
            return switch (method) {
                case "GET" -> doGet(key);
                case "PUT" -> doPut(key, body);
                case "DELETE" -> doDelete(key);
                default -> new ReplicaResult(500, EMPTY);
            };
        } catch (IOException | IllegalArgumentException e) {
            return ReplicaResult.UNAVAILABLE;
        }
    }

    private ReplicaResult doGet(String key) throws IOException {
        try {
            return new ReplicaResult(200, localDao.get(key));
        } catch (NoSuchElementException e) {
            return new ReplicaResult(404, EMPTY);
        }
    }

    private ReplicaResult doPut(String key, byte[] body) throws IOException {
        localDao.upsert(key, body);
        return new ReplicaResult(201, EMPTY);
    }

    private ReplicaResult doDelete(String key) throws IOException {
        localDao.delete(key);
        return new ReplicaResult(202, EMPTY);
    }
}
