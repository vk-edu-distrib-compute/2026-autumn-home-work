package company.vk.edu.distrib.compute.annapiatkova.kv;

import company.vk.edu.distrib.compute.kv.KVService;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

class ReplicatedKVService implements KVService {
    HttpServer server;
    ReplicatedKVServiceHandler handler;

    ReplicatedKVService(int port) throws IOException {
        Path path = Paths.get("/tmp/dao-log-port-" + port + ".tmp");
        handler = new ReplicatedKVServiceHandler(new ByteArrayDao(path), port);
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 0);
        server.createContext("/", handler);
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(1);
    }
}
