package company.vk.edu.distrib.compute.cloudyy74.kv;

import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactoryTest;

@RemoteDaoFactoryTest
public class Cloudyy74RemoteDaoFactory implements RemoteDaoFactory<String> {
    @Override
    public Cloudyy74RemoteDao create(int... ports) {
        return new Cloudyy74RemoteDao(ports[0]);
    }
}
