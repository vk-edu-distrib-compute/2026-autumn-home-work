package company.vk.edu.distrib.compute.akhunzianov.kv;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.ClusterDaoFactoryTest;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;

@ClusterDaoFactoryTest
public class ClusterDaoFactory implements RemoteDaoFactory<String> {

    @Override
    public Dao<String> create(int... ports) {
        return new ClusterDao(ports);
    }
}
