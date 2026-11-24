package company.vk.edu.distrib.compute.flighen.moderation;

import company.vk.edu.distrib.compute.moderation.ModerationService;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactory;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactoryTest;
import company.vk.edu.distrib.compute.moderation.ModerationDeadLetterTest;

import java.io.IOException;

@ModerationServiceFactoryTest
@ModerationDeadLetterTest
public class FlighenModerationServiceFactory implements ModerationServiceFactory {
    @Override
    public ModerationService create(String bootstrapServers) throws IOException {
        return new FlighenModerationService(bootstrapServers);
    }
}
