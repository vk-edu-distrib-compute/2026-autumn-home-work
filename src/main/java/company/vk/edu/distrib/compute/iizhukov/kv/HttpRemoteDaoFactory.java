package company.vk.edu.distrib.compute.iizhukov.kv;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactoryTest;

@RemoteDaoFactoryTest
public final class HttpRemoteDaoFactory implements RemoteDaoFactory<String> {
    @Override
    public Dao<String> create(int... ports) {
        if (ports.length == 0) {
            throw new IllegalArgumentException("Port is required");
        }

        int port = ports[0];
        return new HttpDao(key -> port);
    }
}
