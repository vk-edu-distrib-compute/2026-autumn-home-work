package company.vk.edu.distrib.compute.playingpeano.kv;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.ClusterDaoFactoryTest;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;

@ClusterDaoFactoryTest
public final class ClusterStringDaoFactory implements RemoteDaoFactory<String> {
    @Override
    public Dao<String> create(int... ports) {
        if (ports.length == 0) {
            throw new IllegalArgumentException("At least one KV service port is required");
        }
        for (int port : ports) {
            if (port <= 0 || port >= 65_536) {
                throw new IllegalArgumentException("Port out of range");
            }
        }
        return new ClusterStringDao(ports);
    }
}
