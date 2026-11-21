package company.vk.edu.distrib.compute.iizhukov.urlshortener.api.v0;

import java.io.IOException;
import java.util.NoSuchElementException;

import company.vk.edu.distrib.compute.iizhukov.shared.http.BaseController;
import company.vk.edu.distrib.compute.iizhukov.shared.http.HttpStatus;
import company.vk.edu.distrib.compute.iizhukov.shared.http.Request;
import company.vk.edu.distrib.compute.iizhukov.shared.http.Response;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.db.StorageException;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.db.dao.LinksDao;

public class IndexController extends BaseController<String> {
    public IndexController(int port) {
        super(port);
        setDao(LinksDao.create());
    }

    @Override
    public String path() {
        return "/";
    }

    @Override
    public Response get(Request request) {
        var key = request.path().substring(path().length());

        try {
            return Response.builder()
                    .setStatus(HttpStatus.MOVED_PERMANENTLY)
                    .addHeader("Location", dao().get(key))
                    .build();
        } catch (NoSuchElementException e) {
            return Response.builder()
                    .setStatus(HttpStatus.NOT_FOUND)
                    .build();
        } catch (IOException e) {
            throw new StorageException("cant read link", e);
        }
    }
}
