package company.vk.edu.distrib.compute.akhunzianov.kv;

import java.io.IOException;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactoryTest;

@RemoteDaoFactoryTest
public class RemoteDaoFactoryImpl implements RemoteDaoFactory<String> {

    @Override
    public Dao<String> create(int... ports) throws IOException {
        if (ports.length == 0) {
            throw new IllegalArgumentException("Need a port for the KV service");
        }
        var service = new KVServiceFactory().create(ports[0]);
        service.start();
        return new RemoteDao(ports[0], service);
    }
}
