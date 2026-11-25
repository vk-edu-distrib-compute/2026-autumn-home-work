package company.vk.edu.distrib.compute.randomrandoms.factory;

import company.vk.edu.distrib.compute.moderation.ModerationDeadLetterTest;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactory;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactoryTest;
import company.vk.edu.distrib.compute.randomrandoms.moderation.Moderator;

import java.io.IOException;

@ModerationServiceFactoryTest
@ModerationDeadLetterTest
public class ModeratorFactory implements ModerationServiceFactory {
    @Override
    public ModerationService create(String bootstrapServers) throws IOException {
        return new Moderator(bootstrapServers);
    }
}
