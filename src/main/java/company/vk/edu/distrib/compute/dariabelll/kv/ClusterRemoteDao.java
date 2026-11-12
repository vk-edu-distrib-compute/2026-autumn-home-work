package company.vk.edu.distrib.compute.dariabelll.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.NoSuchElementException;

public class ClusterRemoteDao implements Dao<String> {

    private final RemoteDao[] remoteDaos;
    private final RendezvousHashing hashing;

    public ClusterRemoteDao(int... ports) {
        int[] nodePorts = ports.clone();
        this.remoteDaos = new RemoteDao[nodePorts.length];
        for (int i = 0; i < nodePorts.length; ++i) {
            remoteDaos[i] = new RemoteDao(nodePorts[i]);
        }
        this.hashing = new RendezvousHashing(ports);
    }

    @Override
    public String get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        return remoteDaos[hashing.selectPortIndices(key, 1)[0]].get(key);
    }

    @Override
    public void upsert(String key, String value) throws IllegalArgumentException, IOException {
        remoteDaos[hashing.selectPortIndices(key, 1)[0]].upsert(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        remoteDaos[hashing.selectPortIndices(key, 1)[0]].delete(key);
    }

    @Override
    public void close() throws IOException {
        for (RemoteDao remoteDao : remoteDaos) {
            remoteDao.close();
        }
    }

}
