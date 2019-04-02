package examples.aeron;

import examples.Utils;
import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.Configuration;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.BufferClaim;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.agrona.IoUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

public class SimplePublication {

  private static final Logger logger = LoggerFactory.getLogger(SimplePublication.class);

  private static final String address = "localhost";
  private static final int port = 13000;
  private static final int controlPort = 13001;
  private static final int STREAM_ID = 0xcafe0000;

  private static final ChannelUriStringBuilder outboundChannelBuilder =
      new ChannelUriStringBuilder()
          .endpoint(address + ':' + port)
          .reliable(Boolean.TRUE)
          .media("udp");

  private static final ChannelUriStringBuilder inboundChannelBuilder =
      new ChannelUriStringBuilder()
          .controlEndpoint(address + ':' + controlPort)
          .controlMode(CommonContext.MDC_CONTROL_MODE_DYNAMIC)
          .reliable(Boolean.TRUE)
          .media("udp");

  private static final int FRAGMENT_LIMIT = 8;

  public static void main(String[] args) {
    // setup environment

    String aeronDirName = Utils.tmpFileName("aeron");

    MediaDriver.Context mediaContext =
        new MediaDriver.Context()
            .aeronDirectoryName(aeronDirName)
            .mtuLength(Configuration.MTU_LENGTH)
            .imageLivenessTimeoutNs(
                Duration.ofNanos(Configuration.IMAGE_LIVENESS_TIMEOUT_NS).toNanos())
            .dirDeleteOnStart(true);

    MediaDriver mediaDriver = MediaDriver.launchEmbedded(mediaContext);

    Aeron.Context aeronContext = new Aeron.Context();

    aeronContext.aeronDirectoryName(mediaDriver.aeronDirectoryName());

    Aeron aeron = Aeron.connect(aeronContext);

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  File aeronDirectory = aeronContext.aeronDirectory();
                  if (aeronDirectory.exists()) {
                    IoUtil.delete(aeronDirectory, true);
                  }
                }));

    logger.debug(
        "has initialized embedded media driver, aeron directory: {}",
        aeronContext.aeronDirectoryName());

    // Aeron client ready for creating subscriptions and publications

    String outboundChannel = outboundChannelBuilder.build();

    Publication publication = aeron.addExclusivePublication(outboundChannel, STREAM_ID);

    int sessionId = publication.sessionId();

    String inboundChannel = inboundChannelBuilder.sessionId(sessionId).build();

    Subscription subscription =
        aeron.addSubscription(
            inboundChannel,
            STREAM_ID,
            image ->
                logger.debug(
                    "onClientImageAvailable: {} {}",
                    Integer.toHexString(image.sessionId()),
                    image.sourceIdentity()),
            image ->
                logger.debug(
                    "onClientImageUnavailable: {} {}",
                    Integer.toHexString(image.sessionId()),
                    image.sourceIdentity()));

    AtomicInteger counter = new AtomicInteger();

    Flux.interval(Duration.ofSeconds(1))
        .doOnNext(
            i -> {
              String msg = "Hello from client: " + counter.getAndIncrement();
              logger.debug(
                  "send for sessionId:{}, msg: {}",
                  Integer.toHexString(publication.sessionId()),
                  msg);
              ByteBuffer byteBuffer = ByteBuffer.wrap(msg.getBytes());
              long result = send(publication, byteBuffer);
              if (result < 0) {
                logger.warn(
                    "server failure to send for sessionId:{}, msg: {}, result {}",
                    Integer.toHexString(publication.sessionId()),
                    msg,
                    result);
              }

              subscription.poll(
                  (buffer, offset, length, header) -> {
                    ByteBuffer dstBuffer = ByteBuffer.allocate(length);
                    buffer.getBytes(offset, dstBuffer, length);
                    dstBuffer.flip();

                    String content = StandardCharsets.UTF_8.decode(dstBuffer).toString();
                    logger.debug(
                        "server receive for sessionId:{}, position: {}, msg: {}",
                        header.position(),
                        Integer.toHexString(sessionId),
                        content);
                  },
                  FRAGMENT_LIMIT);
            })
        .blockLast();
  }

  private static long send(Publication publication, ByteBuffer msgBody) {
    int msgLength = msgBody.remaining();
    int position = msgBody.position();
    int limit = msgBody.limit();

    if (msgLength < publication.maxPayloadLength()) {
      BufferClaim bufferClaim = new BufferClaim();
      long result = publication.tryClaim(msgLength, bufferClaim);
      if (result > 0) {
        try {
          MutableDirectBuffer dstBuffer = bufferClaim.buffer();
          int index = bufferClaim.offset();
          dstBuffer.putBytes(index, msgBody, position, limit);
          bufferClaim.commit();
        } catch (Exception ex) {
          bufferClaim.abort();
        }
      }
      return result;
    } else {
      return publication.offer(new UnsafeBuffer(msgBody, position, limit));
    }
  }
}
