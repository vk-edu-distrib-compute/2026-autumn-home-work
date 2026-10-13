package company.vk.edu.distrib.compute.ddkudrin.kv;

import java.io.IOException;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.ClusterDaoFactoryTest;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;

@ClusterDaoFactoryTest
public class DDKudrinClusterRemoteDaoFactory implements RemoteDaoFactory<String> {
    private static final int MIN_PORT_COUNT = 1;

    @Override
    public Dao<String> create(int... ports) throws IOException {
        if (ports.length < MIN_PORT_COUNT) {
            throw new IllegalArgumentException("Expected at least one KV service port");
        }
        return new DDKudrinClusterRemoteDao(ports);
    }
}
