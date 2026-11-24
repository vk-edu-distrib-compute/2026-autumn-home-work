package company.vk.edu.distrib.compute.akravchenya.kv;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.ClusterDaoFactoryTest;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;

import java.io.IOException;

/**
 * Создает {@link RemoteStringClusterDao}, распределяющий ключи между
 * узлами кластера KV service по rendezvous hashing.
 */
@ClusterDaoFactoryTest
public class RemoteStringClusterDaoFactory implements RemoteDaoFactory<String> {

    @Override
    public Dao<String> create(int... ports) throws IOException {
        if (ports.length == 0) {
            throw new IllegalArgumentException("at least one port is required");
        }
        return new RemoteStringClusterDao(ports);
    }
}
