package company.vk.edu.distrib.compute.sghdjsdfhgfj.kv;

import java.io.IOException;

@FunctionalInterface
public interface MyRemoteDaoFunction<T> {
    T apply(MyRemoteDao dao) throws IOException;
}
