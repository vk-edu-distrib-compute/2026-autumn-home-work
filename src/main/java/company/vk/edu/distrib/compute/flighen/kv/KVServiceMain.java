package company.vk.edu.distrib.compute.flighen.kv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class KVServiceMain {
    private static final List<Integer> PORTS = List.of(8081, 8082, 8083);
    private static final int REPLICA_FACTOR = 3;
    private static final int THREADS = 32;

    private KVServiceMain() {
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        System.setProperty("CLUSTER", "8081,8082,8083");

        List<FlighenKVService> services = new ArrayList<>();
        for (int port : PORTS) {
            services.add(new FlighenKVService(port, THREADS, REPLICA_FACTOR));
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> stopServices(services)));

        for (FlighenKVService service : services) {
            service.start();
        }

        Thread.currentThread().join();
    }

    private static void stopServices(List<FlighenKVService> services) {
        for (FlighenKVService service : services) {
            service.stop();
        }
    }
}
