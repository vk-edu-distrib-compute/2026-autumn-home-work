package company.vk.edu.distrib.compute.sghdjsdfhgfj.kv;

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.kv.KVService;
import company.vk.edu.distrib.compute.sghdjsdfhgfj.CustomHttpHandler;
import company.vk.edu.distrib.compute.sghdjsdfhgfj.CustomHttpHandlerTranslator;
import company.vk.edu.distrib.compute.sghdjsdfhgfj.PersistentDao;
import company.vk.edu.distrib.compute.sghdjsdfhgfj.urlshortener.RequestUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;

public class MyKVService implements KVService {
    private final HttpServer server;

    public MyKVService(int port) throws IOException {
        InetSocketAddress addr = new InetSocketAddress(port);
        server = HttpServer.create(addr, 0);
        Path tempDir = Files.createTempDirectory("sghdjsdfhgfj");
        PersistentDao storage = new PersistentDao(tempDir.resolve(RequestUtils.generateId()));

        addContext("/v0/status", new StatusHandler());
        addContext("/v0/entity", new StorageHandler(storage));
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
    }

    private HttpContext addContext(String path, CustomHttpHandler handler) {
        return server.createContext(path, new CustomHttpHandlerTranslator(handler));
    }
}
