package company.vk.edu.distrib.compute.akhunzianov.moderation;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;

import company.vk.edu.distrib.compute.akhunzianov.InMemoryDao;
import company.vk.edu.distrib.compute.akhunzianov.urlshortener.UrlShortenerServiceImpl;
import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ModerationTest {

    private static final String LINK_ID = "abc123";
    private static final String UNREACHABLE_KAFKA = "localhost:1";

    @Test
    void eventsPreserveTextAndTimestamps() {
        var created = new LinkCreatedEvent(LINK_ID, "https://example.com/путь?q=a+b&other=%09", Instant.now());
        var result = new ModerationResultEvent(created.id(), false, "reason\twith\nseparators", Instant.now());
        assertEquals(created, EventCodec.created(EventCodec.encode(created)));
        assertEquals(result, EventCodec.moderated(EventCodec.encode(result)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "poison {{{", "created-v1\t\t\t2026-11-16T00:00:00Z",
        "created-v1\tYWJj\t!!!!\t2026-11-16T00:00:00Z"})
    void rejectsMalformedCreationEvents(String value) {
        assertThrows(IllegalArgumentException.class, () -> EventCodec.created(value));
    }

    @Test
    void rejectsInvalidDecisionInsteadOfDeletingLink() {
        assertThrows(IllegalArgumentException.class,
            () -> EventCodec.moderated("moderated-v1\tYWJj\tgarbage\t\t2026-11-16T00:00:00Z"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"https://example.com/offense", "https://example.com/CRUELTY",
        "https://example.com/%6Fbscene?q=1", "https://example.com/?q=ObScEnE"})
    void rejectsBannedWordsIncludingEncodedAndMixedCase(String url) throws IOException {
        var result = new UrlModerator().check(new LinkCreatedEvent(LINK_ID, url, Instant.now()));
        assertFalse(result.passed());
        assertEquals(LINK_ID, result.id());
        assertFalse(result.reason().isBlank());
    }

    @Test
    void acceptsCleanUrl() throws IOException {
        var result = new UrlModerator().check(
            new LinkCreatedEvent(LINK_ID, "https://example.com/hello?q=world", Instant.now()));
        assertTrue(result.passed());
    }

    @Test
    void kafkaConfigurationIsOnlyAllowedBeforeStart() throws IOException {
        int port;
        try (var socket = new java.net.ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        var service = new UrlShortenerServiceImpl(port, new InMemoryDao(), null);
        service.start();
        try {
            assertThrows(IllegalStateException.class, () -> service.setKafkaBootstrapServers(UNREACHABLE_KAFKA));
        } finally {
            service.stop();
        }
        assertThrows(IllegalStateException.class, () -> service.setKafkaBootstrapServers(UNREACHABLE_KAFKA));
    }

    @Test
    void moderationLifecycleIsSingleUse() throws IOException {
        var service = new ModerationServiceFactory().create(UNREACHABLE_KAFKA);
        assertThrows(IllegalStateException.class, service::stop);
        service.start();
        try {
            assertThrows(IllegalStateException.class, service::start);
        } finally {
            service.stop();
        }
        assertThrows(IllegalStateException.class, service::stop);
        assertThrows(IllegalStateException.class, service::start);
    }

    @Test
    void unavailableKafkaDoesNotBlockHttpOrShutdown() {
        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            int port;
            try (var socket = new java.net.ServerSocket(0)) {
                port = socket.getLocalPort();
            }
            var service = new UrlShortenerServiceImpl(port, new InMemoryDao(), null);
            service.setKafkaBootstrapServers(UNREACHABLE_KAFKA);
            service.start();
            try (var client = HttpClient.newHttpClient()) {
                for (int i = 0; i < 20; i++) {
                    var request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/v0/links"))
                        .POST(HttpRequest.BodyPublishers.ofString("https://example.com/" + i)).build();
                    var response = client.send(request, HttpResponse.BodyHandlers.ofString());
                    assertEquals(201, response.statusCode());
                    var redirect = client.send(HttpRequest.newBuilder(URI.create(response.body())).GET().build(),
                        HttpResponse.BodyHandlers.discarding());
                    assertEquals(301, redirect.statusCode());
                }
            } finally {
                service.stop();
            }
        });
    }
}
