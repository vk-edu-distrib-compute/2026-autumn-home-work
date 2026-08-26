package company.vk.edu.distrib.compute.test.urlshortener;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.test.TestUtils;
import company.vk.edu.distrib.compute.test.TestUtils.Credentials;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.ArgumentsSource;

import static company.vk.edu.distrib.compute.test.TestUtils.CONTENT_TYPE_TEXT;
import static company.vk.edu.distrib.compute.test.TestUtils.TEST_CREDENTIALS;
import static company.vk.edu.distrib.compute.test.TestUtils.TEST_LONG_LINK;
import static company.vk.edu.distrib.compute.test.TestUtils.TEST_LONG_LINK_2;
import static company.vk.edu.distrib.compute.test.TestUtils.TIMEOUT;
import static company.vk.edu.distrib.compute.test.TestUtils.create;
import static company.vk.edu.distrib.compute.test.TestUtils.delete;
import static company.vk.edu.distrib.compute.test.TestUtils.extractId;
import static company.vk.edu.distrib.compute.test.TestUtils.get;
import static company.vk.edu.distrib.compute.test.TestUtils.header;
import static company.vk.edu.distrib.compute.test.TestUtils.randomPort;
import static company.vk.edu.distrib.compute.test.TestUtils.status;
import static company.vk.edu.distrib.compute.test.TestUtils.update;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Authentication tests for {@link UrlShortenerService} implementation.
 *
 */
@ParameterizedClass(allowZeroInvocations = true)
@ArgumentsSource(AuthenticatedUrlShortenerServiceFactoryArgumentsProvider.class)
@EnabledIfEnvironmentVariable(named = "CURRENT_DATE", matches = "2026-09-\\d\\d")
class AuthenticationTest {
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    @Parameter
    AbstractHttpServiceFactory<? extends UrlShortenerService> serviceFactory;

    @AfterAll
    public static void afterAll() {
        HTTP_CLIENT.close();
    }

    @Test
    void createUserDoesNotRequireAuthentication() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            int port = randomPort();
            var service = serviceFactory.create(port);
            try {
                service.start();
                assertEquals(200, createUser(port, TEST_CREDENTIALS).statusCode());
            } finally {
                service.stop();
            }
        });
    }

    @Test
    void protectedEndpointsRequireAuthentication() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            int port = randomPort();
            var service = serviceFactory.create(port);
            try {
                service.start();

                assertEquals(200, createUser(port, TEST_CREDENTIALS).statusCode());

                HttpResponse<String> createResponse = create(HTTP_CLIENT, port, TEST_LONG_LINK);
                assertUnauthorized(createResponse);

                String id = extractId(port, create(HTTP_CLIENT, port, TEST_LONG_LINK, TEST_CREDENTIALS).body());

                assertUnauthorized(get(HTTP_CLIENT, port, id));
                assertUnauthorized(update(HTTP_CLIENT, port, id, TEST_LONG_LINK));
                assertUnauthorized(delete(HTTP_CLIENT, port, id));
            } finally {
                service.stop();
            }
        });
    }

    @Test
    void invalidCredentialsRejected() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            int port = randomPort();
            var service = serviceFactory.create(port);
            try {
                service.start();

                Credentials validCredentials = TEST_CREDENTIALS;
                assertEquals(200, createUser(port, validCredentials).statusCode());

                Credentials invalidCredentials = new Credentials(validCredentials.username(), "oops");
                assertUnauthorized(create(HTTP_CLIENT, port, TEST_LONG_LINK, invalidCredentials));
            } finally {
                service.stop();
            }
        });
    }

    @Test
    void authenticatedLifecycle() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            int port = randomPort();
            var service = serviceFactory.create(port);
            try {
                service.start();

                assertEquals(200, createUser(port, TEST_CREDENTIALS).statusCode());

                String originalLink = TEST_LONG_LINK;
                String updatedLink = TEST_LONG_LINK_2;

                HttpResponse<String> createResponse = create(HTTP_CLIENT, port, originalLink, TEST_CREDENTIALS);
                assertEquals(201, createResponse.statusCode());
                assertEquals(CONTENT_TYPE_TEXT, header(createResponse, "Content-Type"));

                String id = extractId(port, createResponse.body());

                HttpResponse<String> getResponse = get(HTTP_CLIENT, port, id, TEST_CREDENTIALS);
                assertEquals(200, getResponse.statusCode());
                assertEquals(CONTENT_TYPE_TEXT, header(getResponse, "Content-Type"));
                assertEquals(originalLink, getResponse.body());

                assertEquals(200, update(HTTP_CLIENT, port, id, updatedLink, TEST_CREDENTIALS).statusCode());
                assertEquals(updatedLink, get(HTTP_CLIENT, port, id, TEST_CREDENTIALS).body());

                assertEquals(202, delete(HTTP_CLIENT, port, id, TEST_CREDENTIALS).statusCode());
                assertEquals(404, get(HTTP_CLIENT, port, id, TEST_CREDENTIALS).statusCode());
            } finally {
                service.stop();
            }
        });
    }

    private static HttpResponse<Void> createUser(int port, Credentials credentials)
        throws IOException, InterruptedException, URISyntaxException {
        HttpRequest request = HttpRequest.newBuilder()
            .POST(HttpRequest.BodyPublishers.ofString(credentials.username() + ":" + credentials.password()))
            .uri(new URI("http://localhost:" + port + "/internal/users"))
            .header("Content-Type", CONTENT_TYPE_TEXT)
            .timeout(TIMEOUT)
            .build();
        return HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.discarding());
    }

    private static void assertUnauthorized(HttpResponse<?> response) {
        assertEquals(401, response.statusCode());
    }
}
