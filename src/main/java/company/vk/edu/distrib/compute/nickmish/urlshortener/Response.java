package company.vk.edu.distrib.compute.nickmish.urlshortener;

import company.vk.edu.distrib.compute.nickmish.HttpStatus;

public record Response(HttpStatus status, String body, String location) {
    public Response(HttpStatus status, String body) {
        this(status, body, "");
    }

    public Response(HttpStatus status) {
        this(status, "", "");
    }

    public int statusCode() {
        return status.code();
    }

    public boolean hasLocation() {
        return !location.isEmpty();
    }
}
