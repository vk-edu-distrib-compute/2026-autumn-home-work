package company.vk.edu.distrib.compute.mperikov.kv;

import java.io.IOException;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.KVService;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactoryTest;

@RemoteDaoFactoryTest
public final class HttpRemoteDaoFactory implements RemoteDaoFactory<String> {
    @Override
    public Dao<String> create(int... ports) throws IOException {
        if (ports.length == 0) {
            throw new IllegalArgumentException("KV port is required");
        }
        if (singleNode(ports)) {
            return new RemoteStringDao(ports);
        }
        return startCluster(ports);
    }

    private static boolean singleNode(int... ports) {
        return ports.length == 1;
    }

    private static RemoteStringDao startCluster(int... ports) throws IOException {
        KvServiceFactory nodes = new KvServiceFactory();
        KVService[] started = new KVService[ports.length];
        for (int index = 0; index < ports.length; index++) {
            started[index] = nodes.create(ports[index]);
            started[index].start();
        }
        return new RemoteStringDao(ports, started);
    }
}
