package company.vk.edu.distrib.compute.netheer.kv;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.KVService;
import company.vk.edu.distrib.compute.netheer.urlshortener.UrlShortenerServiceFactory;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class LoadTestLauncher {
    private static final int[] SINGLE_PORT = {8081};
    private static final int[] CLUSTER_PORTS = {8081, 8082, 8083};
    private static final int URL_SERVICE_PORT = 8080;
    private static final int EXPECTED_ARGUMENT_COUNT = 1;

    public static void main(String[] args) throws IOException {
        KVServiceFactory factory = new KVServiceFactory();
        List<KVService> nodes = new ArrayList<>();

        if (args.length != EXPECTED_ARGUMENT_COUNT) {
            throw new IllegalArgumentException("Expected argument: single or cluster");
        }
        int[] ports;
        if ("single".equals(args[0])) {
            ports = SINGLE_PORT;
        } else if ("cluster".equals(args[0])) {
            ports = CLUSTER_PORTS;
        } else {
            throw new IllegalArgumentException("Unknown argument");
        }

        for (int port : ports) {
            nodes.add(factory.create(port));
        }

        Dao<String> dao;
        if ("single".equals(args[0])) {
            RemoteDaoFactoryImplementation daoFactory = new RemoteDaoFactoryImplementation();
            dao = daoFactory.create(ports);
        } else {
            ClusterDaoFactoryImplementation daoFactory = new ClusterDaoFactoryImplementation();
            dao = daoFactory.create(ports);
        }
        UrlShortenerServiceFactory urlFactory = new UrlShortenerServiceFactory();
        UrlShortenerService service = urlFactory.create(URL_SERVICE_PORT);
        service.setLinksDao(dao);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
           try {
               service.stop();
           } finally {
               for (KVService node : nodes) {
                   node.stop();
               }
           }
        }));

        for (KVService node : nodes) {
            node.start();
        }
        service.start();
    }

    private LoadTestLauncher() {
    }
}
