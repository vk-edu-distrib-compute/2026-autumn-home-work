package company.vk.edu.distrib.compute.netheer.kv;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactoryTest;

@RemoteDaoFactoryTest
public final class RemoteDaoFactoryImplementation
        implements RemoteDaoFactory<String> {
    private static final int REQUIRED_PORT_COUNT = 1;
    private static final int MIN_PORT = 1;
    private static final int MAX_PORT_EXCLUSIVE = 65536;

    @Override
    public Dao<String> create(int... ports) {
        if (ports.length != REQUIRED_PORT_COUNT) {
            throw new IllegalArgumentException(
                    "Exactly one KV service port is required"
            );
        }

        int port = ports[0];
        if (port < MIN_PORT || port >= MAX_PORT_EXCLUSIVE) {
            throw new IllegalArgumentException("Port out of range");
        }

        return new RemoteDao(port);
    }
}
