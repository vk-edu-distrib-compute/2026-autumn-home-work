package company.vk.edu.distrib.compute;

public enum StatusCode {
    OK(200),
    CREATED(201),
    ACCEPTED(202),
    BAD_REQUEST(400),
    UNAUTHORIZED(401),
    NOT_FOUND(404),
    METHOD_NOT_ALLOWED(405),
    UNPROCESSABLE_ENTITY(422);

    private final int code;

    StatusCode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
