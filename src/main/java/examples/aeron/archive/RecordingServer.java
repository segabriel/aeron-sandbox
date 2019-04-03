package examples.aeron.archive;

import examples.Utils;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingEventsAdapter;
import io.aeron.archive.client.RecordingEventsListener;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

public class RecordingServer {

  private static final Logger logger = LoggerFactory.getLogger(RecordingServer.class);

  static final String INCOMING_RECORDING_ENDPOINT = "localhost:7373";
  static final int INCOMING_RECORDING_STREAM_ID = 3333;

  static final String INCOMING_RECORDING_URI =
      new ChannelUriStringBuilder()
          .endpoint(INCOMING_RECORDING_ENDPOINT)
          .reliable(Boolean.TRUE)
          .media(CommonContext.UDP_MEDIA)
          .build();
  private static final int FRAGMENT_LIMIT = 10;

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) {
    String aeronDirName = Utils.tmpFileName("aeron");
    String archiveDirName = aeronDirName + "-archive";

    try (ArchivingMediaDriver archivingMediaDriver =
            ArchivingMediaDriver.launch(
                new MediaDriver.Context()
                    .threadingMode(ThreadingMode.SHARED)
                    .spiesSimulateConnection(true)
                    .errorHandler(Throwable::printStackTrace)
                    .aeronDirectoryName(aeronDirName)
                    .dirDeleteOnStart(true),
                new Archive.Context()
                    .aeronDirectoryName(aeronDirName)
                    .archiveDirectoryName(archiveDirName)
                    .threadingMode(ArchiveThreadingMode.SHARED)
                    .errorHandler(Throwable::printStackTrace)
                    .fileSyncLevel(0)
                    .deleteArchiveOnStart(true));
        AeronArchive aeronArchive =
            AeronArchive.connect(new AeronArchive.Context().aeronDirectoryName(aeronDirName))) {
      print(archivingMediaDriver);

      // it's important to bind necessary port
      aeronArchive
          .context()
          .aeron()
          .addSubscription(INCOMING_RECORDING_URI, INCOMING_RECORDING_STREAM_ID);

      logger.info(
          "Creating recording to {}, stream id: {}",
          INCOMING_RECORDING_URI,
          INCOMING_RECORDING_STREAM_ID);

      aeronArchive.startRecording(
          INCOMING_RECORDING_URI, INCOMING_RECORDING_STREAM_ID, SourceLocation.REMOTE);

      Flux.interval(Duration.ofSeconds(5))
          .flatMap(
              i ->
                  ArchiveUtils.findRecording(
                      aeronArchive, INCOMING_RECORDING_URI, INCOMING_RECORDING_STREAM_ID, 0, 1000))
          .distinct(recordingDescriptor -> recordingDescriptor.recordingId)
          .log("fond new Recording ")
          .subscribe();

      // subscribe to recording events to get all events
      Subscription recordingEvents =
          aeronArchive
              .context()
              .aeron()
              .addSubscription(
                  aeronArchive.context().recordingEventsChannel(),
                  aeronArchive.context().recordingEventsStreamId());

      RecordingEventsAdapter recordingEventsAdapter =
          new RecordingEventsAdapter(
              new RecordingEventsListenerImpl(), recordingEvents, FRAGMENT_LIMIT);

      Flux.interval(Duration.ofSeconds(1)).map(i -> recordingEventsAdapter.poll()).blockLast();

    } finally {
      Utils.removeFile(archiveDirName);
      Utils.removeFile(aeronDirName);
    }
  }

  private static void print(ArchivingMediaDriver archivingMediaDriver) {
    MediaDriver mediaDriver = archivingMediaDriver.mediaDriver();
    Archive archive = archivingMediaDriver.archive();
    Archive.Context context = archivingMediaDriver.archive().context();

    logger.info("Archive threadingMode: " + context.threadingMode());
    logger.info("Archive controlChannel: " + context.controlChannel());
    logger.info("Archive controlStreamId: " + context.controlStreamId());
    logger.info("Archive localControlChannel: " + context.localControlChannel());
    logger.info("Archive localControlStreamId: " + context.localControlStreamId());
    logger.info("Archive recordingEventsChannel: " + context.recordingEventsChannel());
    logger.info("Archive recordingEventsStreamId: " + context.recordingEventsStreamId());
    logger.info("Archive controlTermBufferSparse: " + context.controlTermBufferSparse());
    logger.info("Archive archiveDirName: " + archive.context().archiveDirectoryName());
    logger.info("Archive aeronDirectoryName: " + mediaDriver.aeronDirectoryName());

    logger.info(
        "Archive listen: "
            + INCOMING_RECORDING_ENDPOINT
            + ", streamId: "
            + INCOMING_RECORDING_STREAM_ID);
  }

  private static class RecordingEventsListenerImpl implements RecordingEventsListener {
    @Override
    public void onStart(
        long recordingId,
        long startPosition,
        int sessionId,
        int streamId,
        String channel,
        String sourceIdentity) {

      logger.info(
          "onStart event, recordingId: {}, startPosition: {}, sessionId {}, streamId: {}, channel: {}, sourceIdentity: {}",
          recordingId,
          startPosition,
          sessionId,
          streamId,
          channel,
          sourceIdentity);
    }

    @Override
    public void onProgress(long recordingId, long startPosition, long position) {
      logger.info(
          "onProgress event, recordingId: {}, startPosition: {}, position: {}",
          recordingId,
          startPosition,
          position);
    }

    @Override
    public void onStop(long recordingId, long startPosition, long stopPosition) {
      logger.info(
          "onProgress event, recordingId: {}, startPosition: {}, stopPosition: {}",
          recordingId,
          startPosition,
          stopPosition);
    }
  }
}
