package company.vk.edu.distrib.compute.iizhukov.kv;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.HttpService;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.ApplicationFactory;

public final class ClusterLauncher {
    private static final int MIN_ARGUMENTS = 2;

    private ClusterLauncher() {
    }

    static void main(String... args) throws IOException {
        if (args.length < MIN_ARGUMENTS) {
            throw new IllegalArgumentException("not all args was provided");
        }

        int[] ports = Arrays.stream(args).mapToInt(Integer::parseInt).toArray();
        var dao = new ClusterRemoteDaoFactory().create(Arrays.copyOfRange(ports, 1, ports.length));
        try {
            var factory = new ApplicationFactory();
            var service = factory.create(ports[0]);

            service.setLinksDao(dao);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> stop(service, dao), "cluster-shutdown"));
            service.start();
        } catch (IOException | RuntimeException e) {
            dao.close();
            throw e;
        }
    }

    private static void stop(HttpService service, Dao<String> dao) {
        try (dao) {
            service.stop();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
