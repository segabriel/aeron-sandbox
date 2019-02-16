package com.github.segabriel.aeron.raw;

import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Client {

  private static final Logger logger = LoggerFactory.getLogger(AeronResources.class);

  private static final String address = "localhost";
  private static final int port = 13000;
  private static final int controlPort = 13001;

  public static void main(String[] args) {
    AeronResources aeronResources = AeronResources.start();
    new Client(aeronResources).start();
  }

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

  private final AtomicInteger counter = new AtomicInteger();
  private final AeronResources resources;

  Client(AeronResources resources) {
    this.resources = resources;
  }

  void start() {
    resources
        .scheduler()
        .schedule(
            () -> {
              String outboundChannel = outboundChannelBuilder.build();

              Publication publication =
                  resources
                      .aeron()
                      .addExclusivePublication(outboundChannel, AeronResources.STREAM_ID);

              int sessionId = publication.sessionId();

              String inboundChannel = inboundChannelBuilder.sessionId(sessionId).build();

              Subscription subscription =
                  resources
                      .aeron()
                      .addSubscription(
                          inboundChannel,
                          AeronResources.STREAM_ID,
                          this::onClientImageAvailable,
                          this::onClientImageUnavailable);

              setupSendAndReceiveTasks(sessionId, subscription, publication);
            });
  }

  private void onClientImageAvailable(Image image) {
    logger.debug(
        "onClientImageAvailable: {} {}",
        Integer.toHexString(image.sessionId()),
        image.sourceIdentity());
  }

  private void onClientImageUnavailable(Image image) {
    logger.debug(
        "onClientImageUnavailable: {} {}",
        Integer.toHexString(image.sessionId()),
        image.sourceIdentity());
  }

  private void setupSendAndReceiveTasks(
      int sessionId, Subscription subscription, Publication publication) {

    boolean needToSend = true;
    boolean needToReceive = false;

    Duration sendInterval = Duration.ofMillis(1);
    Duration receiveInterval = Duration.ofMillis(10);

    if (needToReceive) {
      resources
          .scheduler()
          .schedulePeriodically(
              () -> {
                subscription
                    .images()
                    .forEach(
                        image -> {
                          image.poll(
                              (buffer, offset, length, header) -> {
                                ByteBuffer dstBuffer = ByteBuffer.allocate(length);
                                buffer.getBytes(offset, dstBuffer, length);
                                dstBuffer.flip();

                                String msg = StandardCharsets.UTF_8.decode(dstBuffer).toString();
                                logger.debug(
                                    "client receive for sessionId:{}, msg: {}",
                                    Integer.toHexString(image.sessionId()),
                                    msg);
                              },
                              8);
                        });
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
                String msg = "Hello_" + counter.getAndIncrement();
                logger.debug(
                    "client send for sessionId:{}, msg: {}", Integer.toHexString(sessionId), msg);

                long result = AeronUtils.send(publication, ByteBuffer.wrap(msg.getBytes()));
                if (result < 0) {
                  logger.warn(
                      "client failure to send for sessionId:{}, msg: {}, result {}",
                      Integer.toHexString(sessionId),
                      msg,
                      result);
                }
              },
              Duration.ofSeconds(1).toMillis(),
              sendInterval.toMillis(),
              TimeUnit.MILLISECONDS);
    }
  }
}
