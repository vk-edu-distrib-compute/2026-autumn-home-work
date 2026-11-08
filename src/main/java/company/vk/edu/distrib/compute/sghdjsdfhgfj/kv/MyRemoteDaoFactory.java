package company.vk.edu.distrib.compute.sghdjsdfhgfj.kv;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactoryTest;

import java.io.IOException;

@RemoteDaoFactoryTest
public class MyRemoteDaoFactory implements RemoteDaoFactory<String> {
    @Override
    public Dao<String> create(int... ports) throws IOException {
        int port = ports[0];
        return new MyRemoteDao(port);
    }
}
