package company.vk.edu.distrib.compute.fedorkhokhryakov.kv;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactoryTest;

@RemoteDaoFactoryTest
public class KVRemoteDaoFactory implements RemoteDaoFactory<String> {
    private static final int EXPECTED_PORTS = 1;

    @Override
    public Dao<String> create(int... ports) {
        if (ports.length != EXPECTED_PORTS) {
            throw new IllegalArgumentException("Exactly one KV service port is expected, got " + ports.length);
        }
        return new KVRemoteDao(ports[0]);
    }
}
