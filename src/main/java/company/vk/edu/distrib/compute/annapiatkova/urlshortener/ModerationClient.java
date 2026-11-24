package company.vk.edu.distrib.compute.annapiatkova.urlshortener;

interface ModerationClient {
    void start();

    void stop();
    
    void newLink(String id, String link);
}
