package company.vk.edu.distrib.compute.urlshortener;

import com.sun.net.httpserver.HttpServer;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummyUrlShortenerService implements UrlShortenerService {
    private static final Logger log = LoggerFactory.getLogger(DummyUrlShortenerService.class);

    @Nullable
    private final HttpServer server;

    public DummyUrlShortenerService() {
        server = null;
        initServer();
    }

    private void initServer() {
        if (server == null) {
            log.error("Server is null");
        }
    }

    @Override
    public void start() {
        log.info("Started");
        justSleep(3_000);
    }

    private static void justSleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        log.info("Stopping");
    }
}
