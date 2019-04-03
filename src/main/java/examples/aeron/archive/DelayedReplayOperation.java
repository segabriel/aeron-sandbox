package examples.aeron.archive;

import examples.Utils;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.driver.ThreadingMode;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class DelayedReplayOperation {

  private static final Logger logger = LoggerFactory.getLogger(DelayedReplayOperation.class);

  private static final String CONTROL_RESPONSE_CHANNEL_URI =
      new ChannelUriStringBuilder()
          .endpoint("localhost:8484")
          .reliable(Boolean.TRUE)
          .media(CommonContext.UDP_MEDIA)
          .build();
  private static final int CONTROL_RESPONSE_STREAM_ID = 8484;

  private static final int REPLAY_STREAM_ID = 7474;
  private static final String REPLAY_URI =
      new ChannelUriStringBuilder()
          .endpoint("localhost:7474")
          .reliable(Boolean.TRUE)
          .media(CommonContext.UDP_MEDIA)
          .build();

  private static final long RECORDING_ID = 0;
  private static final long START_POSITION = 64 * 50;

  private static final int FRAGMENT_LIMIT = 1000;
  public static final Duration RETRY_INTERVAL = Duration.ofSeconds(1);
  public static final long RETRY_COUNT = 60;

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) {
    String aeronDirName = Utils.tmpFileName("aeron");

    try (MediaDriver mediaDriver =
            MediaDriver.launch(
                new Context()
                    .threadingMode(ThreadingMode.SHARED)
                    .spiesSimulateConnection(true)
                    .errorHandler(Throwable::printStackTrace)
                    .aeronDirectoryName(aeronDirName)
                    .dirDeleteOnStart(true));
        AeronArchive aeronArchive =
            AeronArchive.connect(
                new AeronArchive.Context()
                    .controlResponseChannel(CONTROL_RESPONSE_CHANNEL_URI)
                    .controlResponseStreamId(CONTROL_RESPONSE_STREAM_ID)
                    .aeronDirectoryName(aeronDirName))) {

      logger.info(
          "Start replay for recordingId: {}, startPosition: {}", RECORDING_ID, START_POSITION);

      Mono.fromRunnable(
              () ->
                  aeronArchive.startReplay(
                      RECORDING_ID,
                      START_POSITION,
                      AeronArchive.NULL_LENGTH,
                      REPLAY_URI,
                      REPLAY_STREAM_ID))
          .retryBackoff(RETRY_COUNT, RETRY_INTERVAL, RETRY_INTERVAL)
          .doOnError(
              ex ->
                  logger.warn(
                      "Can't start replay with the specified start position: {}", START_POSITION))
          .block();

      logger.info(
          "Creating replay subscription to {}, stream id: {}", REPLAY_URI, REPLAY_STREAM_ID);

      Subscription subscription =
          aeronArchive
              .context()
              .aeron()
              .addSubscription(
                  REPLAY_URI,
                  REPLAY_STREAM_ID,
                  DelayedReplayOperation::printAvailableImage,
                  DelayedReplayOperation::printUnavailableImage);

      Flux.interval(Duration.ofSeconds(1))
          .doOnNext(
              i ->
                  subscription.poll(
                      (buffer, offset, length, header) -> {
                        final byte[] data = new byte[length];
                        buffer.getBytes(offset, data);
                        String content = new String(data);

                        logger.debug(
                            "Message from session {} ({}@{}) <<{}>>, header{ pos: {}, offset: {}, termOffset: {}, type: {}",
                            header.sessionId(),
                            length,
                            offset,
                            content,
                            header.position(),
                            header.offset(),
                            header.termOffset(),
                            header.type());
                      },
                      FRAGMENT_LIMIT))
          .blockLast();

    } finally {
      Utils.removeFile(aeronDirName);
    }
  }

  private static void printAvailableImage(final Image image) {
    final Subscription subscription = image.subscription();
    System.out.println(
        String.format(
            "Available image on %s streamId=%d sessionId=%d from %s",
            subscription.channel(),
            subscription.streamId(),
            image.sessionId(),
            image.sourceIdentity()));
  }

  private static void printUnavailableImage(final Image image) {
    final Subscription subscription = image.subscription();
    System.out.println(
        String.format(
            "Unavailable image on %s streamId=%d sessionId=%d",
            subscription.channel(), subscription.streamId(), image.sessionId()));
  }
}
