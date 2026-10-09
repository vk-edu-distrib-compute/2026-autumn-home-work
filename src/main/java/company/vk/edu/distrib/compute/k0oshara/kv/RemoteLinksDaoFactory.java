package company.vk.edu.distrib.compute.k0oshara.kv;

import java.io.IOException;
import java.net.http.HttpClient;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactoryTest;

@RemoteDaoFactoryTest
public final class RemoteLinksDaoFactory implements RemoteDaoFactory<String> {
    @Override
    public Dao<String> create(int... ports) throws IOException {
        if (ports == null || ports.length != 1) {
            throw new IllegalArgumentException("Exactly one port is required");
        }
        if (ports[0] <= 0 || ports[0] >= 65536) {
            throw new IllegalArgumentException("Port out of range");
        }
        return new RemoteLinksDao(ports[0], HttpClient.newHttpClient());
    }
}
