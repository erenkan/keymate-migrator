package io.keymate;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.jboss.logging.Logger;

@ApplicationScoped
public class MainStartup {

    private static final Logger LOG = Logger.getLogger(MainStartup.class);

    final ProcessingLoop loop;
    final SegmentedOutboxRequeueBuffer outboxBuffer;

    public MainStartup(ProcessingLoop loop, SegmentedOutboxRequeueBuffer outboxBuffer) {
        this.loop = loop;
        this.outboxBuffer = outboxBuffer;
    }

    void onStart(@Observes StartupEvent ev) {
        LOG.info("Starting processing loop on boot...");
        loop.start();
        outboxBuffer.start();
    }

    void onStop(@Observes ShutdownEvent ev) {
        LOG.info("Stopping processing loop...");
        outboxBuffer.stop();
        loop.stop();
    }
}
