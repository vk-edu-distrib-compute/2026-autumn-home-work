package company.vk.edu.distrib.compute.netheer.kv;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.ClusterDaoFactoryTest;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;

import java.util.HashSet;
import java.util.Set;

@ClusterDaoFactoryTest
public class ClusterDaoFactoryImplementation implements RemoteDaoFactory<String> {
    private static final int MIN_PORT_VALUE = 1;
    private static final int MAX_PORT_VALUE = 65535;

    @Override
    public Dao<String> create(int... ports) {
        if (ports.length == 0) {
            throw new IllegalArgumentException("Need at least 1 port");
        }

        Set<Integer> seenPorts = new HashSet<>();
        for (int port : ports) {
            if (!seenPorts.add(port)) {
                throw new IllegalArgumentException("Duplicate port");
            }
            if (port < MIN_PORT_VALUE || port > MAX_PORT_VALUE) {
                throw new IllegalArgumentException("Port must be between 1 and 65535");
            }
        }

        return new ClusterDao(ports);
    }
}
