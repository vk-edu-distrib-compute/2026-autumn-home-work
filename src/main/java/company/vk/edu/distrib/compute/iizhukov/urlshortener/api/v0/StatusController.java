package company.vk.edu.distrib.compute.iizhukov.urlshortener.api.v0;

import company.vk.edu.distrib.compute.iizhukov.urlshortener.api.helpers.BaseController;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.api.helpers.HttpStatus;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.api.helpers.Request;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.api.helpers.Response;

public class StatusController extends BaseController {
    public StatusController(int port) {
        super(port);
    }

    @Override
    public String path() {
        return "/v0/status";
    }

    @Override
    public Response get(Request request) {
        return Response.builder()
                .setStatus(HttpStatus.OK)
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
