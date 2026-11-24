package company.vk.edu.distrib.compute.annapiatkova.moderation;

import company.vk.edu.distrib.compute.moderation.ModerationService;

class ModerationServiceImpl implements ModerationService {
    private final ModerationServiceWorker worker;

    ModerationServiceImpl(String bootstrapServers) {
        this.worker = new ModerationServiceWorker(bootstrapServers);
    }

    @Override
    public void start() {
        Thread t = new Thread(worker);
        t.start();
    }

    @Override
    public void stop() {
        worker.shutdown();
    }
}
