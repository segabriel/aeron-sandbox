package com.github.segabriel.aeron.raw;

import io.aeron.ChannelUriStringBuilder;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Server {

  private static final Logger logger = LoggerFactory.getLogger(AeronResources.class);

  private static final String address = "localhost";
  private static final int port = 13000;
  private static final int controlPort = 13001;

  public static void main(String[] args) {
    AeronResources aeronResources = AeronResources.start();
    new Server(aeronResources).start();
  }

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

  private final AtomicInteger counter = new AtomicInteger();
  private final AeronResources resources;

  public Server(AeronResources resources) {
    this.resources = resources;
  }

  private volatile Subscription acceptSubscription;
  private final Map<Integer, Publication> publications = new ConcurrentHashMap<>();

  private void start() {
    resources
        .scheduler()
        .schedule(
            () -> {
              acceptSubscription =
                  resources
                      .aeron()
                      .addSubscription(
                          acceptUri,
                          AeronResources.STREAM_ID,
                          this::onAcceptImageAvailable,
                          this::onAcceptImageUnavailable);

              setupSendAndReceiveTasks();
            });
  }

  private void onAcceptImageAvailable(Image image) {
    int sessionId = image.sessionId();
    String outboundChannel = outboundChannelBuilder.sessionId(sessionId).build();

    logger.debug(
        "onImageAvailable: {} {}, create outbound {}",
        Integer.toHexString(image.sessionId()),
        image.sourceIdentity(),
        outboundChannel);

    resources
        .scheduler()
        .schedule(
            () -> {
              Publication publication =
                  resources
                      .aeron()
                      .addExclusivePublication(outboundChannel, AeronResources.STREAM_ID);
              publications.put(sessionId, publication);
            });
  }

  private void onAcceptImageUnavailable(Image image) {
    int sessionId = image.sessionId();

    logger.debug(
        "onImageUnavailable: {} {}", Integer.toHexString(sessionId), image.sourceIdentity());

    resources
        .scheduler()
        .schedule(
            () -> {
              Publication publication = publications.remove(sessionId);
              if (publication != null) {
                publication.close();
              }
            });
  }

  private void setupSendAndReceiveTasks() {

    boolean needToSend = false;
    boolean needToReceive = true;

    Duration sendInterval = Duration.ofMillis(1);
    Duration receiveInterval = Duration.ofMillis(10);

    if (needToReceive) {
      resources
          .scheduler()
          .schedulePeriodically(
              () -> {
                acceptSubscription
                    .images()
                    .forEach(
                        image ->
                            image.poll(
                                (buffer, offset, length, header) -> {
                                  ByteBuffer dstBuffer = ByteBuffer.allocate(length);
                                  buffer.getBytes(offset, dstBuffer, length);
                                  dstBuffer.flip();

                                  String msg = StandardCharsets.UTF_8.decode(dstBuffer).toString();
                                  logger.debug(
                                      "server receive for sessionId:{}, msg: {}",
                                      Integer.toHexString(image.sessionId()),
                                      msg);
                                },
                                8));
              },
              Duration.ofSeconds(1).toMillis(),
              receiveInterval.toMillis(),
              TimeUnit.MILLISECONDS);
    }

    if (needToSend) {
      resources
          .scheduler()
          .schedulePeriodically(
              () -> {
                publications.forEach(
                    (sessionId, publication) -> {
                      String msg = "Hello_" + counter.getAndIncrement();
                      logger.debug(
                          "send for sessionId:{}, msg: {}", Integer.toHexString(sessionId), msg);
                      long result = AeronUtils.send(publication, ByteBuffer.wrap(msg.getBytes()));
                      if (result < 0) {
                        logger.warn(
                            "server failure to send for sessionId:{}, msg: {}, result {}",
                            Integer.toHexString(sessionId),
                            msg,
                            result);
                      }
                    });
              },
              Duration.ofSeconds(1).toMillis(),
              sendInterval.toMillis(),
              TimeUnit.MILLISECONDS);
    }
  }
}
