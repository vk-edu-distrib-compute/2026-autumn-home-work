package company.vk.edu.distrib.compute.test.urlshortener;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.test.TestUtils;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.ArgumentsSource;

import static company.vk.edu.distrib.compute.test.TestUtils.CONTENT_TYPE_TEXT;
import static company.vk.edu.distrib.compute.test.TestUtils.TEST_LINK_ID;
import static company.vk.edu.distrib.compute.test.TestUtils.TEST_LONG_LINK;
import static company.vk.edu.distrib.compute.test.TestUtils.TEST_LONG_LINK_2;
import static company.vk.edu.distrib.compute.test.TestUtils.TIMEOUT;
import static company.vk.edu.distrib.compute.test.TestUtils.create;
import static company.vk.edu.distrib.compute.test.TestUtils.delete;
import static company.vk.edu.distrib.compute.test.TestUtils.extractId;
import static company.vk.edu.distrib.compute.test.TestUtils.get;
import static company.vk.edu.distrib.compute.test.TestUtils.header;
import static company.vk.edu.distrib.compute.test.TestUtils.randomPort;
import static company.vk.edu.distrib.compute.test.TestUtils.update;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * CRUD tests for {@link UrlShortenerService} implementation.
 */
@ParameterizedClass(allowZeroInvocations = true)
@ArgumentsSource(UrlShortenerServiceFactoryArgumentsProvider.class)
@EnabledIfEnvironmentVariable(named = "CURRENT_DATE", matches = "2026-09-\\d\\d")
class LinksApiTest {
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
    private static final String INVALID_LINK_ID = "invalid-id";
    private static final String INVALID_LONG_LINK = "not-a-valid-link";

    @Parameter
    AbstractHttpServiceFactory<? extends UrlShortenerService> serviceFactory;

    @AfterAll
    public static void afterAll() {
        HTTP_CLIENT.close();
    }

    @Test
    void getAbsent() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            int port = randomPort();
            var service = serviceFactory.create(port);
            try {
                service.start();
                assertEquals(404, get(HTTP_CLIENT, port, TEST_LINK_ID).statusCode());
            } finally {
                service.stop();
            }
        });
    }

    @Test
    void createAndGet() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            int port = randomPort();
            var service = serviceFactory.create(port);
            try {
                service.start();

                String longLink = TEST_LONG_LINK;
                HttpResponse<String> createResponse = create(HTTP_CLIENT, port, longLink);
                assertEquals(201, createResponse.statusCode());
                assertEquals(CONTENT_TYPE_TEXT, header(createResponse, "Content-Type"));

                String id = extractId(port, createResponse.body());

                HttpResponse<String> getResponse = get(HTTP_CLIENT, port, id);
                assertEquals(200, getResponse.statusCode());
                assertEquals(CONTENT_TYPE_TEXT, header(getResponse, "Content-Type"));
                assertEquals(longLink, getResponse.body());
            } finally {
                service.stop();
            }
        });
    }

    @Test
    void createInvalidLink() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            int port = randomPort();
            var service = serviceFactory.create(port);
            try {
                service.start();
                assertEquals(422, create(HTTP_CLIENT, port, INVALID_LONG_LINK).statusCode());
            } finally {
                service.stop();
            }
        });
    }

    @Test
    void getInvalidId() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            int port = randomPort();
            var service = serviceFactory.create(port);
            try {
                service.start();
                assertEquals(422, get(HTTP_CLIENT, port, INVALID_LINK_ID).statusCode());
            } finally {
                service.stop();
            }
        });
    }

    @Test
    void update() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            int port = randomPort();
            var service = serviceFactory.create(port);
            try {
                service.start();

                String originalLink = TEST_LONG_LINK;
                String updatedLink = TEST_LONG_LINK_2;
                String id = extractId(port, create(HTTP_CLIENT, port, originalLink).body());

                assertEquals(200, TestUtils.update(HTTP_CLIENT, port, id, updatedLink).statusCode());
                assertEquals(updatedLink, get(HTTP_CLIENT, port, id).body());
            } finally {
                service.stop();
            }
        });
    }

    @Test
    void updateInvalidId() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            int port = randomPort();
            var service = serviceFactory.create(port);
            try {
                service.start();
                assertEquals(422, TestUtils.update(HTTP_CLIENT, port, INVALID_LINK_ID, TEST_LONG_LINK).statusCode());
            } finally {
                service.stop();
            }
        });
    }

    @Test
    void updateInvalidLink() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            int port = randomPort();
            var service = serviceFactory.create(port);
            try {
                service.start();

                String id = extractId(port, create(HTTP_CLIENT, port, TEST_LONG_LINK).body());
                assertEquals(422, TestUtils.update(HTTP_CLIENT, port, id, INVALID_LONG_LINK).statusCode());
            } finally {
                service.stop();
            }
        });
    }

    @Test
    void updateAbsent() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            int port = randomPort();
            var service = serviceFactory.create(port);
            try {
                service.start();
                TestUtils.delete(HTTP_CLIENT, port, TEST_LINK_ID);
                assertEquals(404, TestUtils.update(HTTP_CLIENT, port, TEST_LINK_ID, TEST_LONG_LINK).statusCode());
            } finally {
                service.stop();
            }
        });
    }

    @Test
    void deleteInvalidId() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            int port = randomPort();
            var service = serviceFactory.create(port);
            try {
                service.start();
                assertEquals(422, TestUtils.delete(HTTP_CLIENT, port, INVALID_LINK_ID).statusCode());
            } finally {
                service.stop();
            }
        });
    }

    @Test
    void delete() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            int port = randomPort();
            var service = serviceFactory.create(port);
            try {
                service.start();

                String id = extractId(port, create(HTTP_CLIENT, port, TEST_LONG_LINK).body());

                assertEquals(202, TestUtils.delete(HTTP_CLIENT, port, id).statusCode());
                assertEquals(404, get(HTTP_CLIENT, port, id).statusCode());
            } finally {
                service.stop();
            }
        });
    }

    @Test
    void deleteAbsent() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            int port = randomPort();
            var service = serviceFactory.create(port);
            try {
                service.start();
                assertEquals(202, TestUtils.delete(HTTP_CLIENT, port, TEST_LINK_ID).statusCode());
            } finally {
                service.stop();
            }
        });
    }

    @Test
    void redirect() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            int port = randomPort();
            var service = serviceFactory.create(port);
            try {
                service.start();

                String id = extractId(port, create(HTTP_CLIENT, port, TEST_LONG_LINK).body());
                HttpRequest request = HttpRequest.newBuilder()
                    .GET()
                    .uri(new URI("http://localhost:%d/%s".formatted(port, id)))
                    .timeout(TIMEOUT)
                    .build();

                HttpResponse<Void> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.discarding());
                assertEquals(301, response.statusCode());
                assertEquals(TEST_LONG_LINK, header(response, "Location"));
            } finally {
                service.stop();
            }
        });
    }

    @Test
    void redirectAbsent() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            int port = randomPort();
            var service = serviceFactory.create(port);
            try {
                service.start();

                HttpRequest request = HttpRequest.newBuilder()
                    .GET()
                    .uri(new URI("http://localhost:%d/%s".formatted(port, "oops")))
                    .timeout(TIMEOUT)
                    .build();

                HttpResponse<Void> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.discarding());
                assertEquals(404, response.statusCode());
            } finally {
                service.stop();
            }
        });
    }

    @Test
    void redirectInvalidId() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            int port = randomPort();
            var service = serviceFactory.create(port);
            try {
                service.start();

                HttpRequest request = HttpRequest.newBuilder()
                    .GET()
                    .uri(new URI("http://localhost:%d/%s".formatted(port, INVALID_LINK_ID)))
                    .timeout(TIMEOUT)
                    .build();

                HttpResponse<Void> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.discarding());
                assertEquals(422, response.statusCode());
            } finally {
                service.stop();
            }
        });
    }
}
