package company.vk.edu.distrib.compute.iizhukov.urlshortener.api.v0;

import company.vk.edu.distrib.compute.iizhukov.shared.http.BaseController;
import company.vk.edu.distrib.compute.iizhukov.shared.http.HttpStatus;
import company.vk.edu.distrib.compute.iizhukov.shared.http.Request;
import company.vk.edu.distrib.compute.iizhukov.shared.http.Response;

public class StatusController extends BaseController<Void> {
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
}
