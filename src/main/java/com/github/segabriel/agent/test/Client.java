package com.github.segabriel.agent.test;

import com.github.segabriel.aeron.raw.AeronResources;
import com.github.segabriel.agent.DynamicCompositeAgent;
import com.github.segabriel.agent.ReactorPublicationAgent;
import com.github.segabriel.agent.ReactorPublicationAgent.DirectBufferHandler;
import com.github.segabriel.agent.ReactorSubscriptionAgent;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
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
  private final DynamicCompositeAgent agent = new DynamicCompositeAgent("client");

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

    IdleStrategy idleStrategy = new SleepingMillisIdleStrategy(400);
    AgentRunner.startOnThread(
        new AgentRunner(idleStrategy, Throwable::printStackTrace, null, agent));
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

    ReactorSubscriptionAgent subscriptionAgent = new ReactorSubscriptionAgent(subscription);
    agent.add(subscriptionAgent);

    subscriptionAgent
        .receive()
        .subscribe(
            buffer -> {
              logger.debug(
                  "client received msg: {}",
                  buffer.getStringWithoutLengthUtf8(0, buffer.capacity()));
            },
            th -> System.out.println("subscriptionAgent error: " + th.getMessage()),
            () -> System.out.println("subscriptionAgent COMPLETED"));

    ReactorPublicationAgent publicationAgent = new ReactorPublicationAgent(publication);
    agent.add(publicationAgent);

    Duration sendInterval = Duration.ofSeconds(1);
    publicationAgent
        .publish(
            Flux.interval(sendInterval)
                .map(i -> "Hello_FROM_CLIENT" + i)
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
  }
}
