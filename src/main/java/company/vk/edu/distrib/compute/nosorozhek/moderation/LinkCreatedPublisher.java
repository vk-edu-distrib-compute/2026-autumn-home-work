package company.vk.edu.distrib.compute.nosorozhek.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;

@FunctionalInterface
public interface LinkCreatedPublisher {
    void publish(LinkCreatedEvent event);
}
