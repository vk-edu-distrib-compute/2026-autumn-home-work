package company.vk.edu.distrib.compute.iizhukov.kv.api.helpers.middlewares;

import java.util.NoSuchElementException;

import company.vk.edu.distrib.compute.iizhukov.shared.http.Handler;
import company.vk.edu.distrib.compute.iizhukov.shared.http.HttpStatus;
import company.vk.edu.distrib.compute.iizhukov.shared.http.Middleware;
import company.vk.edu.distrib.compute.iizhukov.shared.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ErrorHandlingMiddleware implements Middleware {
    private static final Logger log = LoggerFactory.getLogger(ErrorHandlingMiddleware.class);

    @Override
    public Handler apply(Handler handler) {
        return request -> {
            try {
                return handler.handle(request);
            } catch (IllegalArgumentException e) {
                return Response.builder()
                        .setStatus(HttpStatus.BAD_REQUEST)
                        .build();
            } catch (NoSuchElementException e) {
                return Response.builder()
                        .setStatus(HttpStatus.NOT_FOUND)
                        .build();
            } catch (Exception e) {
                log.error("Unhandled exception", e);
                return Response.builder()
                        .setStatus(HttpStatus.INTERNAL_SERVER_ERROR)
                        .build();
            }
        };
    }
}
