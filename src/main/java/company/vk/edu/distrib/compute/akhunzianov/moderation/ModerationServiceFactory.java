package company.vk.edu.distrib.compute.akhunzianov.moderation;

import java.io.IOException;

import company.vk.edu.distrib.compute.moderation.ModerationDeadLetterTest;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactoryTest;

@ModerationServiceFactoryTest
@ModerationDeadLetterTest
public class ModerationServiceFactory implements company.vk.edu.distrib.compute.moderation.ModerationServiceFactory {

    @Override
    public ModerationService create(String bootstrapServers) throws IOException {
        return new ModerationServiceImpl(bootstrapServers, new UrlModerator());
    }
}
