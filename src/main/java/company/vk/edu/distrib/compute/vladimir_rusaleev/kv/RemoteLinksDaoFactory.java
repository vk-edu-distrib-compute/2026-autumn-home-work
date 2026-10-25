package company.vk.edu.distrib.compute.vladimir_rusaleev.kv;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactoryTest;

@RemoteDaoFactoryTest
public final class RemoteLinksDaoFactory implements RemoteDaoFactory<String> {
    private static final int SINGLE_NODE = 1;
    private static final int MIN_PORT = 1;

    @Override
    public Dao<String> create(int... ports) {
        if (ports == null || ports.length != SINGLE_NODE) {
            throw new IllegalArgumentException("Exactly one KV port is required");
        }
        int port = ports[0];
        if (port < MIN_PORT) {
            throw new IllegalArgumentException("Port out of range");
        }
        return new RemoteLinksDao(port);
    }
}
