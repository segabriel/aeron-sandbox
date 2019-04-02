package examples.aeron.archive;

import examples.Utils;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.driver.ThreadingMode;
import java.time.Duration;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;
import reactor.core.publisher.Flux;

public class ExtendedOperation {

  private static final String CONTROL_RESPONSE_CHANNEL_URI =
      new ChannelUriStringBuilder()
          .endpoint("localhost:8485")
          .reliable(Boolean.TRUE)
          .media(CommonContext.UDP_MEDIA)
          .build();
  private static final int CONTROL_RESPONSE_STREAM_ID = 8485;

  private static final String TARGET_RECORDING_URI = RecordingServer.INCOMING_RECORDING_URI;
  private static final int TARGET_RECORDING_STREAM_ID =
      RecordingServer.INCOMING_RECORDING_STREAM_ID;

  private static final ChannelUriStringBuilder EXTENDED_RECORDING_URI_BUILDER =
      new ChannelUriStringBuilder()
          .endpoint("localhost:8486")
          .reliable(Boolean.TRUE)
          .media(CommonContext.UDP_MEDIA);
  private static final int EXTENDED_RECORDING_STREAM_ID = 7777;

  private static final Duration SENT_INTERVAL = Duration.ofSeconds(1);

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) throws InterruptedException {
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

      Flux.interval(Duration.ofSeconds(5))
          .flatMap(
              i ->
                  ArchiveUtils.findRecording(
                      aeronArchive, TARGET_RECORDING_URI, TARGET_RECORDING_STREAM_ID, 0, 1000))
          .distinct(recordingDescriptor -> recordingDescriptor.recordingId)
          .log("fondRecording ")
          .take(1)
          .doOnNext(
              recording -> {
                aeronArchive.stopRecording(recording.originalChannel, recording.streamId);

                aeronArchive.extendRecording(
                    recording.recordingId,
                    EXTENDED_RECORDING_URI_BUILDER.build(),
                    EXTENDED_RECORDING_STREAM_ID,
                    SourceLocation.REMOTE);

                long stopPosition = aeronArchive.getStopPosition(recording.recordingId);

                String channel =
                    EXTENDED_RECORDING_URI_BUILDER
                        .initialPosition(
                            stopPosition, recording.initialTermId, recording.termBufferLength)
                        .build();

                System.out.println(
                    "Creating publication to "
                        + channel
                        + "stream id: "
                        + EXTENDED_RECORDING_STREAM_ID);

                ExclusivePublication publication =
                    aeronArchive
                        .context()
                        .aeron()
                        .addExclusivePublication(channel, EXTENDED_RECORDING_STREAM_ID);

                Flux.interval(SENT_INTERVAL)
                    .map(i -> "extended msg " + i)
                    .doOnNext(message -> send(publication, message))
                    .subscribe();
              })
          .subscribe();

      Thread.currentThread().join();

    } finally {
      Utils.removeFile(aeronDirName);
    }
  }

  private static void send(Publication publication, String body) {
    byte[] messageBytes = body.getBytes();
    UnsafeBuffer buffer =
        new UnsafeBuffer(BufferUtil.allocateDirectAligned(messageBytes.length, 64));
    buffer.putBytes(0, messageBytes);
    long result = publication.offer(buffer);
    String result1;
    if (result > 0) {
      result1 = "Success!";
    } else if (result == Publication.BACK_PRESSURED) {
      result1 = "Offer failed due to back pressure";
    } else if (result == Publication.ADMIN_ACTION) {
      result1 = "Offer failed because of an administration action in the system";
    } else if (result == Publication.NOT_CONNECTED) {
      result1 = "Offer failed because publisher is not connected to subscriber";
    } else if (result == Publication.CLOSED) {
      result1 = "Offer failed publication is closed";
    } else if (result == Publication.MAX_POSITION_EXCEEDED) {
      result1 = "Offer failed due to publication reaching max position";
    } else {
      result1 = "Offer failed due to unknown result code: " + result;
    }
    System.out.println("Offered " + body + " --- " + result1);
  }
}
