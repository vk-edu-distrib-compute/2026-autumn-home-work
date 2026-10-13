package company.vk.edu.distrib.compute.robert.kv;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.ClusterDaoFactoryTest;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;

@ClusterDaoFactoryTest
public class RobertClusterRemoteDaoFactory implements RemoteDaoFactory<String> {
    private static final int MIN_PORTS_COUNT = 1;

    @Override
    public Dao<String> create(int... ports) throws IOException {
        if (ports.length < MIN_PORTS_COUNT) {
            throw new IllegalArgumentException();
        }
        return new RobertClusterRemoteDao<>(
            value -> value.getBytes(StandardCharsets.UTF_8),
            bytes -> new String(bytes, StandardCharsets.UTF_8),
            ports
        );
    }
}
