package company.vk.edu.distrib.compute.iizhukov.kv.api.v0;

import java.util.function.BooleanSupplier;

import company.vk.edu.distrib.compute.iizhukov.shared.http.BaseController;
import company.vk.edu.distrib.compute.iizhukov.shared.http.HttpStatus;
import company.vk.edu.distrib.compute.iizhukov.shared.http.Request;
import company.vk.edu.distrib.compute.iizhukov.shared.http.Response;

public final class StatusController extends BaseController<Void> {
    private final BooleanSupplier available;

    public StatusController(BooleanSupplier available) {
        super(0);
        this.available = available;
    }

    @Override
    public String path() {
        return "/v0/status";
    }

    @Override
    protected boolean matches(Request request) {
        return path().equals(request.path());
    }

    @Override
    public Response get(Request request) {
        return Response.builder()
                .setStatus(available.getAsBoolean()
                        ? HttpStatus.OK
                        : HttpStatus.SERVICE_UNAVAILABLE)
                .build();
    }
}
