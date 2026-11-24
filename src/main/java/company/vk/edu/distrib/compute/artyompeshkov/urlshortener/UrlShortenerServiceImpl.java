package company.vk.edu.distrib.compute.artyompeshkov.urlshortener;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.function.BiConsumer;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.artyompeshkov.moderation.LinkModeration;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;
import org.jspecify.annotations.Nullable;

public class UrlShortenerServiceImpl implements UrlShortenerService {
    private final int port;
    private final Dao<String> users;
    private final HttpServer server;
    private Dao<String> links;
    private boolean started;
    private @Nullable String kafkaBootstrapServers;
    private @Nullable LinkModeration moderation;

    public UrlShortenerServiceImpl(int port, Dao<String> links, Dao<String> users) throws IOException {
        this.port = port;
        this.links = links;
        this.users = users;
        this.server = HttpServer.create();
    }

    @Override
    public void setLinksDao(Dao<String> dao) {
        if (started) {
            throw new IllegalStateException("Links storage can only be set before start");
        }
        links = dao;
    }

    @Override
    public void setKafkaBootstrapServers(String bootstrapServers) {
        if (started) {
            throw new IllegalStateException("Kafka can only be set before start");
        }
        kafkaBootstrapServers = bootstrapServers;
    }

    @Override
    public void start() {
        started = true;
        try {
            server.bind(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to bind to port " + port, e);
        }
        BiConsumer<String, String> onCreated = (id, url) -> { };
        if (kafkaBootstrapServers != null) {
            moderation = new LinkModeration(kafkaBootstrapServers, links);
            onCreated = moderation::publish;
        }
        String baseUrl = "http://localhost:" + port + "/";
        server.createContext(StatusHandler.PATH, new StatusHandler());
        server.createContext(LinksHandler.PATH, new LinksHandler(links, new SimpleAuth(users), baseUrl, onCreated));
        server.createContext(UsersHandler.PATH, new UsersHandler(users));
        server.createContext(RedirectHandler.PATH, new RedirectHandler(links));
        server.start();
        if (moderation != null) {
            moderation.start();
        }
    }

    @Override
    public void stop() {
        started = true;
        server.stop(0);
        if (moderation != null) {
            moderation.close();
        }
        try {
            users.close();
            links.close();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close the storage", e);
        }
    }
}
