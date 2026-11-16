package company.vk.edu.distrib.compute.nickmish.urlshortener;

public record Response(int status, String body, String location) {
    public Response(int status, String body) {
        this(status, body, "");
    }

    public Response(int status) {
        this(status, "", "");
    }

    public boolean hasLocation() {
        return !location.isEmpty();
    }
}
