package company.vk.edu.distrib.compute.robert.kv;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.robert.api.models.HttpStatus;

public class RobertRemoteDao<T> implements Dao<T> {
    private final HttpClient client;
    private final String baseUrl;
    private final boolean isClientOwner;
    private final Function<T, byte[]> encoder;
    private final Function<byte[], T> decoder;

    public RobertRemoteDao(int port, Function<T, byte[]> inputEncoder, Function<byte[], T> inputDecoder) {
        this("http://localhost", port, inputEncoder, inputDecoder);
    }

    public RobertRemoteDao(
        String inputBaseUrl,
        int port,
        Function<T, byte[]> inputEncoder,
        Function<byte[], T> inputDecoder
    ) {
        if (inputBaseUrl == null || inputBaseUrl.isEmpty()) {
            throw new IllegalArgumentException();
        }

        encoder = Objects.requireNonNull(inputEncoder);
        decoder = Objects.requireNonNull(inputDecoder);
        baseUrl = inputBaseUrl + ":" + port;
        client = HttpClient.newHttpClient();
        isClientOwner = true;
    }

    public RobertRemoteDao(
        String inputBaseUrl,
        int port,
        HttpClient inputClient,
        Function<T, byte[]> inputEncoder,
        Function<byte[], T> inputDecoder
    ) {
        if (inputBaseUrl == null || inputBaseUrl.isEmpty() || inputClient == null) {
            throw new IllegalArgumentException();
        }

        encoder = Objects.requireNonNull(inputEncoder);
        decoder = Objects.requireNonNull(inputDecoder);
        baseUrl = inputBaseUrl + ":" + port;
        client = inputClient;
        isClientOwner = false;
    }

    @Override
    public void close() throws IOException {
        if (isClientOwner) {
            client.close();
        }
    }

    @Override
    public T get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        HttpRequest request = HttpRequest.newBuilder(makeUri(key)).GET().build();
        HttpResponse<byte[]> response;
        try {
            response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
        } catch (InterruptedException e) {
            throw new IOException(e);
        }

        if (response.statusCode() == HttpStatus.OK.code()) {
            return decoder.apply(response.body());
        } else if (response.statusCode() == HttpStatus.NOT_FOUND.code()) {
            throw new NoSuchElementException();
        } else {
            throw new IOException();
        }
    }

    @Override
    public void upsert(String key, T value) throws IllegalArgumentException, IOException {
        HttpRequest.BodyPublisher bodyPublisher = HttpRequest.BodyPublishers.ofByteArray(encoder.apply(value));

        HttpRequest request = HttpRequest.newBuilder(makeUri(key)).PUT(bodyPublisher).build();
        HttpResponse<byte[]> response;
        try {
            response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
        } catch (InterruptedException e) {
            throw new IOException(e);
        }

        if (response.statusCode() != HttpStatus.CREATED.code()) {
            throw new IOException();
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        HttpRequest request = HttpRequest.newBuilder(makeUri(key)).DELETE().build();
        HttpResponse<byte[]> response;
        try {
            response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
        } catch (InterruptedException e) {
            throw new IOException(e);
        }

        if (response.statusCode() != HttpStatus.ACCEPTED.code()) {
            throw new IOException();
        }
    }

    private URI makeUri(String key) {
        String encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8);
        return URI.create(baseUrl + "/v0/entity?id=" + encodedKey);
    }
}
