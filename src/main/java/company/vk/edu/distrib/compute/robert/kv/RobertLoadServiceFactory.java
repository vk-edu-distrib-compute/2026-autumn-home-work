package company.vk.edu.distrib.compute.robert.kv;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Function;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.Dao;

public class RobertLoadServiceFactory extends AbstractHttpServiceFactory<RobertLoadService> {
    private static final String PORTS_ENV = "BENCH_KV_PORTS";
    private static final String THREADS_ENV = "BENCH_EXECUTOR_THREADS";
    private static final int DEFAULT_EXECUTOR_THREADS = 8;
    private static final int LEAST_SHARDS_COUNT_NUMBER = 1;

    @Override
    protected RobertLoadService doCreate(int port) throws IOException {
        int[] ports = getPorts(port);
        int executorThreads = getExecutorThreads();
        Dao<byte[]> remoteDao = ports.length == 1
            ? new RobertRemoteDao<>(ports[0], Function.identity(), Function.identity())
            : new RobertClusterRemoteDao<>(Function.identity(), Function.identity(), ports);

        try {
            return new RobertLoadService(port, remoteDao, executorThreads);
        } catch (IOException | RuntimeException e) {
            try {
                remoteDao.close();
            } catch (IOException closeException) {
                e.addSuppressed(closeException);
            }
            throw e;
        }
    }

    private static int[] getPorts(int servicePort) {
        String value = System.getenv(PORTS_ENV);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException();
        }

        int[] ports;
        try {
            ports = Arrays.stream(value.split(",", -1))
                .map(String::strip)
                .mapToInt(Integer::parseInt)
                .toArray();
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(e);
        }

        for (int port : ports) {
            if (port <= 0 || port >= 65536 || port == servicePort) {
                throw new IllegalArgumentException();
            }
        }
        
        return ports;
    }

    private static int getExecutorThreads() {
        String value = System.getenv().getOrDefault(THREADS_ENV, Integer.toString(DEFAULT_EXECUTOR_THREADS));
        int executorThreads;
        try {
            executorThreads = Integer.parseInt(value.strip());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(e);
        }
        if (executorThreads < LEAST_SHARDS_COUNT_NUMBER) {
            throw new IllegalArgumentException();
        }
        return executorThreads;
    }
}
