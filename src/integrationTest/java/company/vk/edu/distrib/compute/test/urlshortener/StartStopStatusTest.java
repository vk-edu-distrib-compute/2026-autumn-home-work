package company.vk.edu.distrib.compute.test.urlshortener;

import java.io.IOException;
import java.net.http.HttpClient;
import java.time.Duration;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.ArgumentsSource;

import static company.vk.edu.distrib.compute.test.TestUtils.TIMEOUT;
import static company.vk.edu.distrib.compute.test.TestUtils.randomPort;
import static company.vk.edu.distrib.compute.test.TestUtils.status;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Basic init/deinit test for {@link UrlShortenerService} implementation.
 *
 */
@ParameterizedClass(allowZeroInvocations = true)
@ArgumentsSource(UrlShortenerServiceFactoryArgumentsProvider.class)
@EnabledIfEnvironmentVariable(named = "CURRENT_DATE", matches = "2026-09-\\d\\d")
class StartStopStatusTest {
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    @Parameter
    AbstractHttpServiceFactory<? extends UrlShortenerService> serviceFactory;

    @AfterAll
    public static void afterAll() {
        HTTP_CLIENT.close();
    }

    @Test
    void create() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            int port = randomPort();
            serviceFactory.create(port);
            assertThrows(IOException.class, () -> status(HTTP_CLIENT, port));
        });
    }

    @Test
    void start() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            int port = randomPort();
            var service = serviceFactory.create(port);
            try {
                service.start();
                assertEquals(200, status(HTTP_CLIENT, port));
            } finally {
                service.stop();
            }
        });
    }

    @Test
    void doubleStartThrows() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            int port = randomPort();
            var service = serviceFactory.create(port);
            try {
                service.start();
                assertThrows(Throwable.class, service::start);
            } finally {
                service.stop();
            }
        });
    }

    @Test
    void stop() {
        assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
            int port = randomPort();
            var service = serviceFactory.create(port);
            service.start();
            service.stop();
            assertThrows(IOException.class, () -> status(HTTP_CLIENT, port));
        });
    }
}
