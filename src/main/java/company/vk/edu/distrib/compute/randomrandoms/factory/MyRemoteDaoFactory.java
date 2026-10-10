package company.vk.edu.distrib.compute.randomrandoms.factory;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.ClusterDaoFactoryTest;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactoryTest;
import company.vk.edu.distrib.compute.randomrandoms.kv.RemoteDao;

import java.io.IOException;

@RemoteDaoFactoryTest
@ClusterDaoFactoryTest
public class MyRemoteDaoFactory implements RemoteDaoFactory<String> {
    @Override
    public Dao<String> create(int... ports) throws IOException {
        return new RemoteDao(ports);
    }
}
