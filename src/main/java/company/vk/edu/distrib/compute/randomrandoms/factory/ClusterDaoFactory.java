package company.vk.edu.distrib.compute.randomrandoms.factory;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.ClusterDaoFactoryTest;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;
import company.vk.edu.distrib.compute.randomrandoms.kv.ClusterDao;

import java.io.IOException;

@ClusterDaoFactoryTest
public class ClusterDaoFactory implements RemoteDaoFactory<String> {
    @Override
    public Dao<String> create(int... ports) throws IOException {
        return new ClusterDao(ports);
    }
}
