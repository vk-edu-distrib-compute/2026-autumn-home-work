package company.vk.edu.distrib.compute.masha533.moderation;

import company.vk.edu.distrib.compute.moderation.ModerationService;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactory;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactoryTest;
import company.vk.edu.distrib.compute.moderation.ModerationDeadLetterTest;

@ModerationDeadLetterTest
@ModerationServiceFactoryTest
public class ModerationServiceFactoryImpl implements ModerationServiceFactory {
    @Override
    public ModerationService create(String bootstrapServers) {
        return new ModerationServiceImpl(bootstrapServers);
    }
}
