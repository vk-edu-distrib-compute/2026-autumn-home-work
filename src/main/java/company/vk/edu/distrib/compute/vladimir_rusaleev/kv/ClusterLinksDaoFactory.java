package company.vk.edu.distrib.compute.vladimir_rusaleev.kv;

import java.util.Arrays;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.ClusterDaoFactoryTest;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;

@ClusterDaoFactoryTest
public final class ClusterLinksDaoFactory implements RemoteDaoFactory<String> {
    private static final int MIN_PORT = 1;

    @Override
    public Dao<String> create(int... ports) {
        if (ports == null || ports.length == 0) {
            throw new IllegalArgumentException("At least one port is required");
        }

        int[] sorted = ports.clone();
        Arrays.sort(sorted);

        for (int i = 0; i < sorted.length; i++) {
            if (sorted[i] < MIN_PORT) {
                throw new IllegalArgumentException("Port out of range");
            }
            if (i > 0 && sorted[i] == sorted[i - 1]) {
                throw new IllegalArgumentException("Duplicate");
            }
        }

        return new ClusterLinksDao(sorted);
    }
}
