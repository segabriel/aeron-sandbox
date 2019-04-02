package examples.aeron;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.driver.Configuration;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.BufferClaim;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.agrona.IoUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class SimpleSubscription {

  private static final Logger logger = LoggerFactory.getLogger(SimpleSubscription.class);

  private static final String address = "localhost";
  private static final int port = 13000;
  private static final int controlPort = 13001;
  private static final int STREAM_ID = 0xcafe0000;

  private static final String acceptUri =
      new ChannelUriStringBuilder()
          .endpoint(address + ':' + port)
          .reliable(Boolean.TRUE)
          .media("udp")
          .build();

  private static final ChannelUriStringBuilder outboundChannelBuilder =
      new ChannelUriStringBuilder()
          .controlEndpoint(address + ':' + controlPort)
          .reliable(Boolean.TRUE)
          .media("udp");
  private static final int FRAGMENT_LIMIT = 8;

  public static void main(String[] args) {
    // setup environment

    String aeronDirName =
        IoUtil.tmpDirName()
            + "aeron"
            + '-'
            + System.getProperty("user.name", "default")
            + '-'
            + UUID.randomUUID().toString();

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

    Scheduler scheduler = Schedulers.parallel();

    logger.debug(
        "has initialized embedded media driver, aeron directory: {}",
        aeronContext.aeronDirectoryName());

    // Aeron client ready for creating subscriptions and publications

    logger.info("accept URI: {}, stream id: {}", acceptUri, STREAM_ID);

    Map<Integer, Publication> publications = new ConcurrentHashMap<>();
    Map<Integer, Image> images = new ConcurrentHashMap<>();

    // we should bind some port to accept incoming requests
    aeron.addSubscription(
        acceptUri,
        STREAM_ID,
        image -> {
          int sessionId = image.sessionId();
          String outboundChannel = outboundChannelBuilder.sessionId(sessionId).build();

          logger.debug(
              "onImageAvailable: {} {}, create outbound {}",
              Integer.toHexString(image.sessionId()),
              image.sourceIdentity(),
              outboundChannel);

          scheduler.schedule(
              () -> {
                Publication publication = aeron.addExclusivePublication(outboundChannel, STREAM_ID);
                publications.put(sessionId, publication);
                images.put(sessionId, image);
              });
        },
        image -> {
          int sessionId = image.sessionId();

          logger.debug(
              "onImageUnavailable: {} {}", Integer.toHexString(sessionId), image.sourceIdentity());

          scheduler.schedule(
              () -> {
                Publication publication = publications.remove(sessionId);
                if (publication != null) {
                  publication.close();
                }
                images.remove(sessionId);
              });
        });

    AtomicInteger counter = new AtomicInteger();

    Flux.interval(Duration.ofSeconds(1))
        .doOnNext(
            i -> {
              for (Publication publication : publications.values()) {
                String msg = "Hello from server: " + counter.getAndIncrement();
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
              }

              for (Image image : images.values()) {
                image.poll(
                    (buffer, offset, length, header) -> {
                      ByteBuffer dstBuffer = ByteBuffer.allocate(length);
                      buffer.getBytes(offset, dstBuffer, length);
                      dstBuffer.flip();

                      String msg = StandardCharsets.UTF_8.decode(dstBuffer).toString();
                      logger.debug(
                          "server receive for sessionId:{}, position: {}, msg: {}",
                          header.position(),
                          Integer.toHexString(image.sessionId()),
                          msg);
                    },
                    FRAGMENT_LIMIT);
              }
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
