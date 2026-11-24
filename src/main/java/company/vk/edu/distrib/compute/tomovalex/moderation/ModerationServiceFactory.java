package company.vk.edu.distrib.compute.tomovalex.moderation;

import company.vk.edu.distrib.compute.moderation.ModerationDeadLetterTest;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactoryTest;

import java.io.IOException;

@ModerationServiceFactoryTest
@ModerationDeadLetterTest
public class ModerationServiceFactory implements company.vk.edu.distrib.compute.moderation.ModerationServiceFactory {
    @Override
    public ModerationService create(String bootstrapServers) throws IOException {
        return new ModerationService(bootstrapServers);
    }
}
