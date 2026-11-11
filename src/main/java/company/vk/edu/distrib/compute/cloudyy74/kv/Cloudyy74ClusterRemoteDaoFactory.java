package company.vk.edu.distrib.compute.cloudyy74.kv;

import company.vk.edu.distrib.compute.kv.ClusterDaoFactoryTest;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;

@ClusterDaoFactoryTest
public class Cloudyy74ClusterRemoteDaoFactory implements RemoteDaoFactory<String> {
    @Override
    public Cloudyy74ClusterRemoteDao create(int... ports) {
        return new Cloudyy74ClusterRemoteDao(ports);
    }
}
