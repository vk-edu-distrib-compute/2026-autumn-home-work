package company.vk.edu.distrib.compute.cloudyy74.moderation;

import company.vk.edu.distrib.compute.moderation.ModerationDeadLetterTest;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactory;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactoryTest;

import java.io.IOException;

@ModerationServiceFactoryTest
@ModerationDeadLetterTest
public class Cloudyy74ModerationServiceFactory implements ModerationServiceFactory {
    @Override
    public Cloudyy74ModerationService create(String bootstrapServers) throws IOException {
        return new Cloudyy74ModerationService(bootstrapServers);
    }
}
