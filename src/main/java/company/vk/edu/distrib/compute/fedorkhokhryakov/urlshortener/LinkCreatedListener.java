package company.vk.edu.distrib.compute.fedorkhokhryakov.urlshortener;

@FunctionalInterface
public interface LinkCreatedListener {
    LinkCreatedListener NONE = (id, url) -> { };

    void linkCreated(String id, String url);
}
