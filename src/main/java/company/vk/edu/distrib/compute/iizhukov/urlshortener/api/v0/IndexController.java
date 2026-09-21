package company.vk.edu.distrib.compute.iizhukov.urlshortener.api.v0;

import company.vk.edu.distrib.compute.iizhukov.urlshortener.api.helpers.BaseController;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.api.helpers.HttpStatus;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.api.helpers.Request;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.api.helpers.Response;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.db.dao.LinksDao;

public class IndexController extends BaseController {
    private final LinksDao dao = LinksDao.create();

    public IndexController(int port) {
        super(port);
    }

    @Override
    public String path() {
        return "/";
    }

    @Override
    public Response get(Request request) {
        var key = request.path().substring(path().length());
        var model = dao.get(key);

        if (model == null) {
            return Response.builder()
                    .setStatus(HttpStatus.NOT_FOUND)
                    .build();
        }

        return Response.builder()
                .setStatus(HttpStatus.MOVED_PERMANENTLY)
                .addHeader("Location", model.url())
                .build();
    }

    @Override
    public Response post(Request request) {
        return null;
    }

    @Override
    public Response put(Request request) {
        return null;
    }

    @Override
    public Response delete(Request request) {
        return null;
    }
}
