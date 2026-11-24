package company.vk.edu.distrib.compute.annapiatkova.moderation;

import company.vk.edu.distrib.compute.moderation.ModerationServiceFactory;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactoryTest;
import company.vk.edu.distrib.compute.moderation.ModerationService;

@ModerationServiceFactoryTest
class ModerationServiceFactoryImpl implements ModerationServiceFactory {
    @Override
    public ModerationService create(String bootstrapServers) {
        return new ModerationServiceImpl(bootstrapServers);
    }
}
