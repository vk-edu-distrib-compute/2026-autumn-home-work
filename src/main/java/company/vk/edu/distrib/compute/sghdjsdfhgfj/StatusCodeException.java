package company.vk.edu.distrib.compute.sghdjsdfhgfj;

import company.vk.edu.distrib.compute.StatusCode;

import java.io.Serial;

public class StatusCodeException extends Exception {
    @Serial
    private static final long serialVersionUID = 6097182046991062671L;
    private final int statusCode;

    public StatusCodeException(int statusCode) {
        super();
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public static StatusCodeException methodNotAllowed() {
        return new StatusCodeException(StatusCode.METHOD_NOT_ALLOWED.getCode());
    }

    public static StatusCodeException unauthorized() {
        return new StatusCodeException(StatusCode.UNAUTHORIZED.getCode());
    }

    public static StatusCodeException unprocessable() {
        return new StatusCodeException(StatusCode.UNPROCESSABLE_ENTITY.getCode());
    }

    public static StatusCodeException notFound() {
        return new StatusCodeException(StatusCode.NOT_FOUND.getCode());
    }

    public static StatusCodeException badRequest() {
        return new StatusCodeException(StatusCode.BAD_REQUEST.getCode());
    }
}
