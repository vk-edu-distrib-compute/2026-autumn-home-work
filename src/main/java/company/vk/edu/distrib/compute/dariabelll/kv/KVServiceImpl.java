package company.vk.edu.distrib.compute.dariabelll.kv;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.kv.KVService;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.util.concurrent.ExecutorService;

public class KVServiceImpl implements KVService {

    private final HttpServer server;
    private final JournaledDao dao;
    private final @Nullable ExecutorService executor;
    private final HttpClient client;

    public KVServiceImpl(
            int port,
            JournaledDao dao,
            @Nullable ExecutorService executor) throws IOException {
        this.dao = dao;
        this.executor = executor;
        server = HttpServer.create(new InetSocketAddress(port), 0);
        client = HttpClient.newHttpClient();
        server.createContext(
                "/",
                new KVHttpHandler(dao, port, client)
        );
        if (executor != null) {
            server.setExecutor(executor);
        }
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        try (dao; executor; client) {
            server.stop(1);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
