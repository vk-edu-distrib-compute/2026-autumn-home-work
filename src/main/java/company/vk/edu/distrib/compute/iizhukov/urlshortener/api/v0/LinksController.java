package company.vk.edu.distrib.compute.iizhukov.urlshortener.api.v0;

import java.util.List;

import company.vk.edu.distrib.compute.iizhukov.urlshortener.api.helpers.BaseController;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.api.helpers.HttpStatus;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.api.helpers.Request;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.api.helpers.Response;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.api.helpers.middlewares.AuthMiddleware;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.db.dao.LinksDao;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.db.models.LinkModel;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.utils.GeneratorUtils;

public class LinksController extends BaseController {
    private final LinksDao dao = LinksDao.create();

    public LinksController(int port) {
        super(port, List.of(new AuthMiddleware()));
    }

    @Override
    public String path() {
        return "/v0/links";
    }

    @Override
    public Response get(Request request) {
        var key = request.path().substring(path().length() + 1);
        var model = dao.get(key);

        if (model == null) {
            return Response.builder()
                    .setStatus(HttpStatus.NOT_FOUND)
                    .build();
        }

        return Response.builder()
                    .setContent(model.url())
                    .setStatus(HttpStatus.OK)
                    .build();
    }

    @Override
    public Response post(Request request) {
        var url = request.body();
        var key = GeneratorUtils.generateKey(10);

        var model = new LinkModel(
                "user",
                url
        );

        dao.upsert(key, model);

        return Response.builder()
                .setStatus(HttpStatus.CREATED)
                .setContent("http://localhost:%s/%s".formatted(port(), key))
                .build();
    }

    @Override
    public Response put(Request request) {
        var key = request.path().substring(path().length() + 1);
        var url = request.body();
        var model = dao.get(key);

        if (model == null) {
            return Response.builder()
                    .setStatus(HttpStatus.NOT_FOUND)
                    .build();
        }

        dao.upsert(key, new LinkModel(
                model.author(),
                url
        ));

        return Response.builder()
                .setStatus(HttpStatus.OK)
                .setContent("http://localhost:%s/%s".formatted(port(), key))
                .build();
    }

    @Override
    public Response delete(Request request) {
        var key = request.path().substring(path().length() + 1);
        var model = dao.get(key);

        if (model != null) {
            dao.delete(key);
        }

        return Response.builder()
                .setStatus(HttpStatus.ACCEPTED)
                .build();
    }
}
