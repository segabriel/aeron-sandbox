package com.github.segabriel.aeron.raw;

import io.aeron.Aeron;
import io.aeron.driver.Configuration;
import io.aeron.driver.MediaDriver;
import java.io.File;
import java.time.Duration;
import java.util.UUID;
import org.agrona.IoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class AeronResources implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(AeronResources.class);

  /** The stream ID that the server and client use for messages. */
  public static final int STREAM_ID = 0xcafe0000;

  private final Inner inner;

  private AeronResources(Inner inner) {
    this.inner = inner;
  }

  public static AeronResources start() {
    return new AeronResources(new Inner());
  }

  public Aeron aeron() {
    return inner.aeron;
  }

  public Scheduler scheduler() {
    return inner.scheduler;
  }

  @Override
  public void close() {
    if (inner != null) {
      inner.aeron.close();
      inner.mediaDriver.close();
    }
  }

  private static class Inner {
    private final Aeron aeron;
    private final MediaDriver mediaDriver;
    private final Scheduler scheduler;

    public Inner() {
      MediaDriver.Context mediaContext =
          new MediaDriver.Context()
              .aeronDirectoryName(generateRandomTmpDirName())
              .mtuLength(Configuration.MTU_LENGTH)
              .imageLivenessTimeoutNs(
                  Duration.ofNanos(Configuration.IMAGE_LIVENESS_TIMEOUT_NS).toNanos())
              .dirDeleteOnStart(true);

      mediaDriver = MediaDriver.launchEmbedded(mediaContext);

      Aeron.Context aeronContext = new Aeron.Context();
      aeronContext.aeronDirectoryName(mediaDriver.aeronDirectoryName());

      aeron = Aeron.connect(aeronContext);

      scheduler = Schedulers.parallel();

      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    File aeronDirectory = aeronContext.aeronDirectory();
                    if (aeronDirectory.exists()) {
                      IoUtil.delete(aeronDirectory, true);
                      logger.debug("{} deleted aeron directory {}", this, aeronDirectory);
                    }
                  }));

      logger.debug(
          "has initialized embedded media driver, aeron directory: {}",
          aeronContext.aeronDirectoryName());
    }

    private static String generateRandomTmpDirName() {
      return IoUtil.tmpDirName()
          + "aeron"
          + '-'
          + System.getProperty("user.name", "default")
          + '-'
          + UUID.randomUUID().toString();
    }
  }
}
