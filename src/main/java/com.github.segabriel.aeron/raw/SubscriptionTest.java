package com.github.segabriel.aeron.raw;

import io.aeron.ChannelUriStringBuilder;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.UnsafeBuffer;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public class SubscriptionTest {

  public static void main(String[] args) throws InterruptedException {
    AeronResources aeronResources = AeronResources.start();

    Scheduler scheduler = aeronResources.scheduler();

    UnsafeBuffer offerBuffer = new UnsafeBuffer(new byte[32]);

    String subChannel1 =
        new ChannelUriStringBuilder()
            .endpoint("localhost:13000")
            .reliable(Boolean.TRUE)
            .media("udp")
            // .sessionId(1)
            .build();
    int streamId1 = 1;
    String subChannel2 =
        new ChannelUriStringBuilder()
            .endpoint("localhost:13000")
            .reliable(Boolean.TRUE)
            .media("udp")
            // .sessionId(2)
            .build();
    int streamId2 = 1;
    String pubChannel1 =
        new ChannelUriStringBuilder()
            .endpoint("localhost:13000")
            .reliable(Boolean.TRUE)
            .media("udp")
            .sessionId(1)
            .build();

    Subscription subscription1 =
        aeronResources
            .aeron()
            .addSubscription(
                subChannel1,
                streamId1,
                image -> System.out.println(image.subscription().channel() + "__1 active image"),
                image -> System.out.println(image.subscription().channel() + "__1 closed"));

    Subscription subscription2 =
        aeronResources
            .aeron()
            .addSubscription(
                subChannel2,
                streamId2,
                image -> System.out.println(image.subscription().channel() + "__2 active image"),
                image -> System.out.println(image.subscription().channel() + "__2 closed"));

    ExclusivePublication publication =
        aeronResources.aeron().addExclusivePublication(pubChannel1, streamId1);

    Disposable disposable1 =
        scheduler.schedulePeriodically(
            () -> {
              if (!subscription1.isClosed()) {
                int workCount =
                    subscription1.poll(
                        (buffer, offset, length, header) ->
                            System.out.println(
                                subscription1.channel()
                                    + "__1, sessionId: "
                                    + header.sessionId()
                                    + ", streamId:"
                                    + header.streamId()
                                    + ", receive "
                                    + length
                                    + " bytes"),
                        10);

                System.out.println(
                    subscription1.channel() + "__1: active images: " + subscription1.imageCount());
              }
            },
            Duration.ofSeconds(1).toMillis(),
            Duration.ofSeconds(1).toMillis(),
            TimeUnit.MILLISECONDS);

    Disposable disposable2 =
        scheduler.schedulePeriodically(
            () -> {
              if (!subscription2.isClosed()) {
                int workCount =
                    subscription2.poll(
                        (buffer, offset, length, header) ->
                            System.out.println(
                                subscription2.channel()
                                    + "__2, sessionId: "
                                    + header.sessionId()
                                    + ", streamId:"
                                    + header.streamId()
                                    + ", receive "
                                    + length
                                    + " bytes"),
                        10);

                System.out.println(
                    subscription1.channel() + "__2: active images: " + subscription1.imageCount());
              }
            },
            Duration.ofSeconds(1).toMillis(),
            Duration.ofSeconds(1).toMillis(),
            TimeUnit.MILLISECONDS);

    Disposable disposable3 =
        scheduler.schedulePeriodically(
            () -> {
              long workCount = publication.offer(offerBuffer);
              if (workCount < 1) {
                System.out.println(publication.channel() + ", code: " + workCount);
              }
            },
            Duration.ofSeconds(1).toMillis(),
            Duration.ofSeconds(1).toMillis(),
            TimeUnit.MILLISECONDS);

    Mono.delay(Duration.ofSeconds(5))
        .doOnSuccess(
            $ -> {
              subscription2.close();
            })
        .block();

    Thread.currentThread().join();
  }
}
