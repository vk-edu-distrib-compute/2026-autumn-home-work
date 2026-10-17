package company.vk.edu.distrib.compute.sghdjsdfhgfj.kv;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.StatusCode;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.NoSuchElementException;

public class MyRemoteDao implements Dao<String> {
    private final URI url;

    public MyRemoteDao(int port) {
        url = URI.create("http://localhost:" + port);
    }

    @Override
    public String get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        HttpURLConnection conn = (HttpURLConnection) url.resolve("/v0/entity?id=" + key).toURL().openConnection();
        conn.setRequestMethod("GET");
        conn.setDoOutput(true);
        int status = conn.getResponseCode();
        if (status == StatusCode.OK.getCode()) {
            byte[] contents = conn.getInputStream().readAllBytes();
            return new String(contents);
        } else if (status == StatusCode.NOT_FOUND.getCode()) {
            throw new NoSuchElementException();
        } else {
            throw new IOException(conn.getResponseMessage());
        }
    }

    @Override
    public void upsert(String key, String value) throws IllegalArgumentException, IOException {
        HttpURLConnection conn = (HttpURLConnection) url.resolve("/v0/entity?id=" + key).toURL().openConnection();
        conn.setRequestMethod("PUT");
        conn.setDoOutput(true);
        conn.getOutputStream().write(value.getBytes());
        conn.getOutputStream().flush();
        conn.getOutputStream().close();
        int status = conn.getResponseCode();
        if (status != StatusCode.CREATED.getCode()) {
            throw new IOException(conn.getResponseMessage());
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        HttpURLConnection conn = (HttpURLConnection) url.resolve("/v0/entity?id=" + key).toURL().openConnection();
        conn.setRequestMethod("DELETE");
        int status = conn.getResponseCode();
        if (status != StatusCode.ACCEPTED.getCode()) {
            throw new IOException(conn.getResponseMessage());
        }
    }

    @Override
    public void close() throws IOException {
        //
    }
}
