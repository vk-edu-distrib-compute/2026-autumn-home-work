package company.vk.edu.distrib.compute.sghdjsdfhgfj.kv;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.StatusCode;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

public class MyRemoteDao implements Dao<String> {
    private final int port;
    private final URI url;

    public MyRemoteDao(int port) {
        this.port = port;
        this.url = URI.create("http://localhost:" + port);
    }

    public int getPort() {
        return port;
    }

    public byte[] get(String key, int ack) throws NoSuchElementException, IllegalArgumentException, IOException {
        HttpURLConnection conn = (HttpURLConnection) formatUrl(key, ack).toURL().openConnection();
        conn.setRequestMethod("GET");
        conn.setDoOutput(true);
        int status = conn.getResponseCode();
        if (status == StatusCode.OK.getCode()) {
            return conn.getInputStream().readAllBytes();
        } else if (status == StatusCode.NOT_FOUND.getCode()) {
            throw new NoSuchElementException();
        } else {
            throw new IOException(conn.getResponseMessage());
        }
    }

    @Override
    public String get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        return new String(get(key, 1), StandardCharsets.UTF_8);
    }

    public void upsert(String key, byte[] value, int ack) throws IllegalArgumentException, IOException {
        HttpURLConnection conn = (HttpURLConnection) formatUrl(key, ack).toURL().openConnection();
        conn.setRequestMethod("PUT");
        conn.setDoOutput(true);
        conn.getOutputStream().write(value);
        conn.getOutputStream().flush();
        conn.getOutputStream().close();
        int status = conn.getResponseCode();
        if (status != StatusCode.CREATED.getCode()) {
            throw new IOException(conn.getResponseMessage());
        }
    }

    @Override
    public void upsert(String key, String value) throws IllegalArgumentException, IOException {
        upsert(key, value.getBytes(StandardCharsets.UTF_8), 1);
    }

    public void delete(String key, int ack) throws IllegalArgumentException, IOException {
        HttpURLConnection conn = (HttpURLConnection) formatUrl(key, ack).toURL().openConnection();
        conn.setRequestMethod("DELETE");
        int status = conn.getResponseCode();
        if (status != StatusCode.ACCEPTED.getCode()) {
            throw new IOException(conn.getResponseMessage());
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        delete(key, 1);
    }

    @Override
    public void close() throws IOException {
        //
    }

    private URI formatUrl(String id, int ack) {
        return url.resolve("/v0/entity?id=" + id + "&ack=" + ack);
    }
}
