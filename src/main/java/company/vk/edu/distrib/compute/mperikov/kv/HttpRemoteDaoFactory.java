package company.vk.edu.distrib.compute.mperikov.kv;

import java.io.IOException;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactoryTest;

@RemoteDaoFactoryTest
public final class HttpRemoteDaoFactory implements RemoteDaoFactory<String> {
    @Override
    public Dao<String> create(int... ports) throws IOException {
        if (ports.length == 0) {
            throw new IllegalArgumentException("KV port is required");
        }
        return new RemoteStringDao(ports[0]);
    }
}
