package company.vk.edu.distrib.compute.robert.kv;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactoryTest;

@RemoteDaoFactoryTest
public class RobertRemoteDaoFactory implements RemoteDaoFactory<String> {
    private static final int MIN_PORTS_COUNT = 1;

    @Override
    public Dao<String> create(int... ports) throws IOException {
        if (ports.length < MIN_PORTS_COUNT) {
            throw new IllegalArgumentException();
        }
        return new RobertRemoteDao<>(
            ports[0],
            value -> value.getBytes(StandardCharsets.UTF_8),
            bytes -> new String(bytes, StandardCharsets.UTF_8)
        );
    }
}
