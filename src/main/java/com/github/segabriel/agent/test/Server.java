package com.github.segabriel.agent.test;

import com.github.segabriel.aeron.raw.AeronResources;
import com.github.segabriel.agent.DynamicCompositeAgent;
import com.github.segabriel.agent.ReactorImageAgent;
import com.github.segabriel.agent.ReactorPublicationAgent;
import com.github.segabriel.agent.ReactorPublicationAgent.DirectBufferHandler;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.Image;
import io.aeron.Publication;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

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

  Server(AeronResources resources) {
    this.resources = resources;
  }

  private final DynamicCompositeAgent agent = new DynamicCompositeAgent("server");

  void start() {
    resources
        .scheduler()
        .schedule(
            () -> {
              resources
                  .aeron()
                  .addSubscription(
                      acceptUri,
                      AeronResources.STREAM_ID,
                      this::onAcceptImageAvailable,
                      this::onAcceptImageUnavailable);
            });

    IdleStrategy idleStrategy = new SleepingMillisIdleStrategy(400);

    AgentRunner.startOnThread(new AgentRunner(idleStrategy, th -> {}, null, agent));
  }

  private void onAcceptImageAvailable(Image image) {
    int sessionId = image.sessionId();
    String outboundChannel = outboundChannelBuilder.sessionId(sessionId).build();

    logger.debug(
        "onImageAvailable: {} {}, create outbound {}",
        Integer.toHexString(image.sessionId()),
        image.sourceIdentity(),
        outboundChannel);

    ReactorImageAgent imageAgent = new ReactorImageAgent(image);
    agent.add(imageAgent);

    imageAgent
        .receive()
        .subscribe(
            buffer -> {
              logger.debug(
                  "server received msg: {}",
                  buffer.getStringWithoutLengthUtf8(0, buffer.capacity()));
            },
            th -> System.out.println("imageAgent error: " + th.getMessage()),
            () -> System.out.println("imageAgent COMPLETED"));

    resources
        .scheduler()
        .schedule(
            () -> {
              Publication publication =
                  resources
                      .aeron()
                      .addExclusivePublication(outboundChannel, AeronResources.STREAM_ID);

              ReactorPublicationAgent publicationAgent = new ReactorPublicationAgent(publication);
              agent.add(publicationAgent);

              Duration sendInterval = Duration.ofSeconds(1);
              publicationAgent
                  .publish(
                      Flux.interval(sendInterval)
                          .map(i -> "Hello__FROM_SERVER" + i)
                          .map(msg -> ByteBuffer.wrap(msg.getBytes())),
                      new DirectBufferHandler<ByteBuffer>() {
                        @Override
                        public int estimateLength(ByteBuffer buffer) {
                          return buffer.capacity();
                        }

                        @Override
                        public DirectBuffer map(ByteBuffer buffer, int length) {
                          return new UnsafeBuffer(buffer);
                        }

                        @Override
                        public void dispose(ByteBuffer buffer) {
                          // no-op
                        }
                      })
                  .subscribe(
                      null,
                      th -> System.out.println("publicationAgent error: " + th.getMessage()),
                      () -> System.out.println("publicationAgent COMPLETED"));
            });
  }

  private void onAcceptImageUnavailable(Image image) {
    logger.debug(
        "onImageUnavailable: {} {}",
        Integer.toHexString(image.sessionId()),
        image.sourceIdentity());
  }
}
