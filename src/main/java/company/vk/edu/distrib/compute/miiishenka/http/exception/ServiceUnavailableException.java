package company.vk.edu.distrib.compute.miiishenka.http.exception;

public class ServiceUnavailableException extends HttpStatusException {
    @Override
    public int getStatusCode() {
        return 503;
    }
}
