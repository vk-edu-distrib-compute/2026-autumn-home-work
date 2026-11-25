package company.vk.edu.distrib.compute.kl1dd.kv;

import java.io.IOException;
import java.util.NoSuchElementException;

public class DaoHelper {
    private final MyKVDao dao;

    public DaoHelper(int port) throws IOException {
        this.dao = new MyKVDao(port);
    }

    boolean delete(String id) {
        try {
            dao.delete(id);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    boolean upsert(String id, byte[] value) {
        try {
            dao.upsert(id, value);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    ReplicationImpl.GetReplicaResult getResult(String id) {
        try {
            byte[] value = dao.get(id);
            return new ReplicationImpl.GetReplicaResult(200, value);
        } catch (NoSuchElementException e) {
            return new ReplicationImpl.GetReplicaResult(404, null);
        } catch (IOException e) {
            return new ReplicationImpl.GetReplicaResult(503, null);
        }
    }
}
