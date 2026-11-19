package company.vk.edu.distrib.compute.dzolin.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.*;

@company.vk.edu.distrib.compute.kv.ClusterDaoFactoryTest
public class ClusterRemoteDaoFactoryImpl implements company.vk.edu.distrib.compute.kv.RemoteDaoFactory<String> {
    @Override
    public Dao<String> create(int... ports) throws IOException {
        return new RemoteDao(ports);
    }

    private static final class RemoteDao implements Dao<String> {
        private final Map<String, Dao<String>> daos;

        public static long getHash(String key, String shardId) {
            long hash = 7_391_827_451L;
            String value = key.length() + ":" + key + shardId;

            for (int i = 0; i < value.length(); i++) {
                hash ^= value.charAt(i);
                hash *= 1_000_003L;
            }

            return hash;
        }

        public String getShard(String key) {
            long max = Long.MIN_VALUE;
            var shard = "";
            for (var port : daos.keySet()) {
                var hash = getHash(key, port);
                if (hash > max) {
                    max = hash;
                    shard = port;
                }
            }
            return shard;
        }

        private RemoteDao(int... ports) throws IOException {
            if (ports.length == 0) {
                throw new IllegalArgumentException("expected at least one port");
            }

            var factory = new RemoteDaoFactoryImpl();
            daos = new HashMap<>();
            for (var port: ports) {
                daos.put(Integer.toString(port), factory.create(port));
            }
        }

        @Override
        public String get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
            var shard = getShard(key);
            var dao = daos.get(shard);
            return dao.get(key);
        }

        @Override
        public void upsert(String key, String value) throws IllegalArgumentException, IOException {
            var shard = getShard(key);
            var dao = daos.get(shard);
            dao.upsert(key, value);
        }

        @Override
        public void delete(String key) throws IllegalArgumentException, IOException {
            var shard = getShard(key);
            var dao = daos.get(shard);
            dao.delete(key);
        }

        @Override
        public void close() throws IOException {
            for (var dao: daos.values()) {
                dao.close();
            }
        }
    }
}
