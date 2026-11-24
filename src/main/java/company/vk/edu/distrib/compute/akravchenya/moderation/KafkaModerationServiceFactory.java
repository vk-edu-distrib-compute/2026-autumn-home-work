package company.vk.edu.distrib.compute.akravchenya.moderation;

import company.vk.edu.distrib.compute.moderation.ModerationDeadLetterTest;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactory;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactoryTest;

import java.io.IOException;

@ModerationServiceFactoryTest
@ModerationDeadLetterTest
public class KafkaModerationServiceFactory implements ModerationServiceFactory {

    @Override
    public ModerationService create(String bootstrapServers) throws IOException {
        return new KafkaModerationService(bootstrapServers);
    }
}
