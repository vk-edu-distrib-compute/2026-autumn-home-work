package company.vk.edu.distrib.compute.akravchenya.kv;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactoryTest;

import java.io.IOException;

/**
 * Создает {@link RemoteStringDao}, обращающийся к сервису хранения по HTTP.
 */
@RemoteDaoFactoryTest
public class RemoteStringDaoFactory implements RemoteDaoFactory<String> {

    @Override
    public Dao<String> create(int... ports) throws IOException {
        if (ports.length == 0) {
            throw new IllegalArgumentException("at least one port is required");
        }
        return new RemoteStringDao(ports);
    }
}
