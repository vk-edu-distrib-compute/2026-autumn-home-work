package company.vk.edu.distrib.compute.ddkudrin.kv;

import java.io.IOException;
import java.util.Locale;
import java.util.NoSuchElementException;

import company.vk.edu.distrib.compute.Dao;

public class DDKudrinClusterRemoteDao implements Dao<String> {
    private final DDKudrinNodeRouter router;
    private final DDKudrinRemoteDao[] nodes;

    public DDKudrinClusterRemoteDao(int... ports) {
        this(getAlgorithm(), ports);
    }

    public DDKudrinClusterRemoteDao(DDKudrinNodeRouter.Algorithm algorithm, int... ports) {
        router = new DDKudrinNodeRouter(algorithm, ports);
        nodes = new DDKudrinRemoteDao[ports.length];
        for (int i = 0; i < ports.length; i++) {
            nodes[i] = new DDKudrinRemoteDao(ports[i]);
        }
    }

    private static DDKudrinNodeRouter.Algorithm getAlgorithm() {
        String value = System.getenv("SHARDING_ALGORITHM");
        if (value == null) {
            return DDKudrinNodeRouter.Algorithm.CONSISTENT;
        }
        try {
            return DDKudrinNodeRouter.Algorithm.valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("SHARDING_ALGORITHM must be CONSISTENT or RENDEZVOUS", e);
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        nodes[router.selectNode(key)].delete(key);
    }

    @Override
    public String get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        return nodes[router.selectNode(key)].get(key);
    }

    @Override
    public void upsert(String key, String value) throws IllegalArgumentException, IOException {
        nodes[router.selectNode(key)].upsert(key, value);
    }

    @Override
    public void close() throws IOException {
        for (DDKudrinRemoteDao node : nodes) {
            node.close();
        }
    }
}
