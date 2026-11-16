package company.vk.edu.distrib.compute.iizhukov.shared.http;

@FunctionalInterface
public interface Handler {
    Response handle(Request request);
}
