package examples.aeron.archive;

import examples.Utils;
import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.driver.ThreadingMode;
import java.time.Duration;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;
import reactor.core.publisher.Flux;

public class Sender {

  private static final String OUTBOUND_CHANNEL_URI =
      new ChannelUriStringBuilder()
          .endpoint(RecordingServer.INCOMING_RECORDING_ENDPOINT)
          .reliable(Boolean.TRUE)
          .media(CommonContext.UDP_MEDIA)
          .build();
  private static final int OUTBOUND_STREAM_ID = RecordingServer.INCOMING_RECORDING_STREAM_ID;

  private static final Duration SENT_INTERVAL = Duration.ofSeconds(1);

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
                    .errorHandler(Throwable::printStackTrace)
                    .aeronDirectoryName(aeronDirName)
                    .dirDeleteOnStart(true));
        Aeron aeron =
            Aeron.connect(
                new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()))) {

      System.out.println(
          "Creating publication to " + OUTBOUND_CHANNEL_URI + ", stream id: " + OUTBOUND_STREAM_ID);

      Publication publication =
          aeron.addExclusivePublication(OUTBOUND_CHANNEL_URI, OUTBOUND_STREAM_ID);

      Flux.interval(SENT_INTERVAL)
          .map(i -> "Hello World! " + i)
          .doOnNext(message -> send(publication, message))
          .blockLast();
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
