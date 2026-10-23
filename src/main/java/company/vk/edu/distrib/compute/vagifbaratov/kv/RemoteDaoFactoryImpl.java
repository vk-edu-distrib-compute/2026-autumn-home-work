package company.vk.edu.distrib.compute.vagifbaratov.kv;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactoryTest;

import java.io.IOException;

@RemoteDaoFactoryTest
public class RemoteDaoFactoryImpl implements RemoteDaoFactory<String> {
    @Override
    public Dao<String> create(int... ports) throws IOException {
        if (ports.length != 1) {
            throw new IllegalArgumentException("Exactly one port is required");
        }
        if (ports[0] <= 0 || ports[0] >= 65536) {
            throw new IllegalArgumentException("Port is invalid");
        }

        return new RemoteDao("http://localhost", ports[0]);
    }
}
