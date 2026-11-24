package company.vk.edu.distrib.compute.mrglaster.urlshortener.factory;

import company.vk.edu.distrib.compute.moderation.ModerationDeadLetterTest;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactory;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactoryTest;
import company.vk.edu.distrib.compute.mrglaster.urlshortener.service.EPKafkaModerationServiceImpl;

import java.io.IOException;

@ModerationServiceFactoryTest
@ModerationDeadLetterTest
public class EPKafkaModerationServiceFactory implements ModerationServiceFactory {

    @Override
    public ModerationService create(String bootstrapServers) throws IOException {
        return new EPKafkaModerationServiceImpl(bootstrapServers);
    }
}
