package company.vk.edu.distrib.compute.virogg.kv;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.ClusterDaoFactoryTest;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;

@ClusterDaoFactoryTest
public final class ClusterRemoteDaoFactoryImpl implements RemoteDaoFactory<String> {
    @Override
    public Dao<String> create(int... ports) {
        return new ClusterRemoteStringDao(ports);
    }
}
