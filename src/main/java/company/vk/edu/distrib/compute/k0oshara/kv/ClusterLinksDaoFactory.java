package company.vk.edu.distrib.compute.k0oshara.kv;

import java.io.IOException;
import java.net.http.HttpClient;
import java.util.Arrays;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.ClusterDaoFactoryTest;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;

@ClusterDaoFactoryTest
public final class ClusterLinksDaoFactory implements RemoteDaoFactory<String> {
    private static final int MAX_PORT = 65535;

    @Override
    public Dao<String> create(int... ports) throws IOException {
        if (ports == null || ports.length == 0) {
            throw new IllegalArgumentException("At least one port is required");
        }
        for (int port : ports) {
            if (port <= 0 || port > MAX_PORT) {
                throw new IllegalArgumentException("Port out of range");
            }
        }
        int[] normalizedPorts = Arrays.stream(ports).distinct().sorted().toArray();
        return new RemoteLinksDao(normalizedPorts, HttpClient.newHttpClient());
    }
}
