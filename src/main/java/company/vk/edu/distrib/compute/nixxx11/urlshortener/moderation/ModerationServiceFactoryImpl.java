package company.vk.edu.distrib.compute.nixxx11.urlshortener.moderation;

import company.vk.edu.distrib.compute.moderation.ModerationDeadLetterTest;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactory;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactoryTest;

@ModerationServiceFactoryTest
@ModerationDeadLetterTest
public class ModerationServiceFactoryImpl implements ModerationServiceFactory {
  @Override
  public ModerationService create(final String bootstrapServers) {
    return new ModerationServiceImpl(bootstrapServers);
  }
}
