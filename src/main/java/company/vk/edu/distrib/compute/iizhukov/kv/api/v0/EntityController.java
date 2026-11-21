package company.vk.edu.distrib.compute.iizhukov.kv.api.v0;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.iizhukov.shared.http.BaseController;
import company.vk.edu.distrib.compute.iizhukov.shared.http.HttpStatus;
import company.vk.edu.distrib.compute.iizhukov.shared.http.Request;
import company.vk.edu.distrib.compute.iizhukov.shared.http.Response;

public final class EntityController extends BaseController<byte[]> {
    private static final String ID = "id=";

    public EntityController(Dao<byte[]> dao) {
        super(0);
        setDao(dao);
    }

    @Override
    public String path() {
        return "/v0/entity";
    }

    @Override
    protected boolean matches(Request request) {
        return path().equals(request.path());
    }

    @Override
    public Response get(Request request) {
        try {
            return Response.builder()
                    .setStatus(HttpStatus.OK)
                    .setContent(dao().get(key(request)))
                    .build();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Response put(Request request) {
        try {
            dao().upsert(key(request), request.rawBody());
            return Response.builder()
                    .setStatus(HttpStatus.CREATED)
                    .build();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Response delete(Request request) {
        try {
            dao().delete(key(request));
            return Response.builder()
                    .setStatus(HttpStatus.ACCEPTED)
                    .build();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String key(Request request) {
        if (!request.query().startsWith(ID)) {
            throw new IllegalArgumentException("Missing id");
        }

        var key = URLDecoder.decode(request.query().substring(ID.length()), StandardCharsets.UTF_8);

        if (key.isEmpty()) {
            throw new IllegalArgumentException("Empty id");
        }

        return key;
    }
}
