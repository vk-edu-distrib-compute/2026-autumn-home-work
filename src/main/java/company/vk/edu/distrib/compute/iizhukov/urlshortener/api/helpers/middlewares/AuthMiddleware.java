package company.vk.edu.distrib.compute.iizhukov.urlshortener.api.helpers.middlewares;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import company.vk.edu.distrib.compute.iizhukov.urlshortener.api.helpers.Handler;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.api.helpers.HttpStatus;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.api.helpers.Middleware;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.api.helpers.Request;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.api.helpers.Response;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.db.dao.UserDao;

public class AuthMiddleware implements Middleware {
    private final UserDao dao = UserDao.create();

    @Override
    public Response handle(Request request, Handler handler) {
        var authPayload = request.headers().get("Authorization");

        if (authPayload == null || !authPayload.startsWith("Basic ")) {
            return Response.builder()
                    .setStatus(HttpStatus.UNAUTHORIZED)
                    .build();
        }
        var token = authPayload.substring("Basic ".length());

        var data = new String(
                Base64.getDecoder().decode(token),
                StandardCharsets.UTF_8
        ).split(":");

        var user = data[0];
        var password = data[1];

        if (dao.get(user) == null) {
            return Response.builder()
                    .setStatus(HttpStatus.UNAUTHORIZED)
                    .build();
        }

        var newHash = Integer.toString(password.hashCode());
        var oldHash = dao.get(user).passwordHash();

        if (!oldHash.equals(newHash)) {
            return Response.builder()
                    .setStatus(HttpStatus.UNAUTHORIZED)
                    .build();
        }

        return handler.handle(request);
    }
}
