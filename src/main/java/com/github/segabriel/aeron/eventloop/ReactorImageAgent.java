package com.github.segabriel.aeron.eventloop;

import io.aeron.Image;
import io.aeron.ImageFragmentAssembler;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.UnsafeBuffer;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;

final class ReactorImageAgent implements Agent {

  private static final Logger logger = LoggerFactory.getLogger(ReactorImageAgent.class);

  private static final AtomicLongFieldUpdater<ReactorImageAgent> REQUESTED =
      AtomicLongFieldUpdater.newUpdater(ReactorImageAgent.class, "requested");

  private static final AtomicReferenceFieldUpdater<ReactorImageAgent, CoreSubscriber>
      DESTINATION_SUBSCRIBER =
          AtomicReferenceFieldUpdater.newUpdater(
              ReactorImageAgent.class, CoreSubscriber.class, "destinationSubscriber");

  private static final CoreSubscriber<? super DirectBuffer> CANCELLED_SUBSCRIBER =
      new CancelledSubscriber();

  private final int fragmentLimit = 10;
  private final FluxReceive inbound = new FluxReceive();
  private final FragmentHandler fragmentHandler =
      new ImageFragmentAssembler(new FragmentHandlerImpl());

  private final Image image;

  private volatile long requested;
  private volatile boolean fastpath;
  private long produced;
  private volatile CoreSubscriber<? super DirectBuffer> destinationSubscriber;

  public ReactorImageAgent(Image image) {
    this.image = Objects.requireNonNull(image, "image cannot be null");
  }

  @Override
  public void onStart() {
    // no-op
  }

  @Override
  public int doWork() {
    if (destinationSubscriber == CANCELLED_SUBSCRIBER) {
      throw new AgentTerminationException();
    }
    if (image.isClosed()) {
      CoreSubscriber destination =
          DESTINATION_SUBSCRIBER.getAndSet(ReactorImageAgent.this, CANCELLED_SUBSCRIBER);
      AgentTerminationException terminationException =
          new AgentTerminationException("image [" + image.sessionId() + "] was closed");
      if (destination != null) {
        destination.onError(terminationException);
      }
      throw terminationException;
    }
    if (fastpath) {
      return image.poll(fragmentHandler, fragmentLimit);
    }
    int r = (int) Math.min(requested, fragmentLimit);
    int fragments = 0;
    if (r > 0) {
      fragments = image.poll(fragmentHandler, r);
      if (produced > 0) {
        Operators.produced(REQUESTED, this, produced);
        produced = 0;
      }
    }
    return fragments;
  }

  @Override
  public void onClose() {
    if (destinationSubscriber != CANCELLED_SUBSCRIBER) {
      CoreSubscriber destination =
          DESTINATION_SUBSCRIBER.getAndSet(ReactorImageAgent.this, CANCELLED_SUBSCRIBER);
      if (destination != null) {
        destination.onError(new AgentTerminationException("onClose was invoked"));
      }
    }
  }

  @Override
  public String roleName() {
    return ReactorImageAgent.class.getName() + ":" + image.sessionId();
  }

  public Flux<DirectBuffer> receive() {
    return inbound;
  }

  private class FragmentHandlerImpl implements FragmentHandler {

    @Override
    public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
      produced++;

      CoreSubscriber<? super DirectBuffer> destination =
          ReactorImageAgent.this.destinationSubscriber;

      destination.onNext(new UnsafeBuffer(buffer, offset, length));
    }
  }

  private class FluxReceive extends Flux<DirectBuffer> implements Subscription {

    @Override
    public void request(long n) {
      if (fastpath) {
        return;
      }
      if (n == Long.MAX_VALUE) {
        fastpath = true;
        requested = Long.MAX_VALUE;
        return;
      }
      Operators.addCap(REQUESTED, ReactorImageAgent.this, n);
    }

    @Override
    public void cancel() {
      CoreSubscriber destination =
          DESTINATION_SUBSCRIBER.getAndSet(ReactorImageAgent.this, CANCELLED_SUBSCRIBER);
      if (destination != null) {
        destination.onComplete();
      }
      logger.debug(
          "Destination subscriber on aeron inbound has been cancelled, session id {}",
          Integer.toHexString(image.sessionId()));
    }

    @Override
    public void subscribe(CoreSubscriber<? super DirectBuffer> destinationSubscriber) {
      boolean result =
          DESTINATION_SUBSCRIBER.compareAndSet(ReactorImageAgent.this, null, destinationSubscriber);
      if (result) {
        destinationSubscriber.onSubscribe(this);
      } else {
        // only subscriber is allowed on receive()
        Operators.error(destinationSubscriber, Exceptions.duplicateOnSubscribeException());
      }
    }
  }

  private static class CancelledSubscriber implements CoreSubscriber<DirectBuffer> {

    @Override
    public void onSubscribe(Subscription s) {
      // no-op
    }

    @Override
    public void onNext(DirectBuffer directBuffer) {
      logger.warn(
          "Received buffer(len={}) which will be dropped immediately due cancelled aeron inbound",
          directBuffer.capacity());
    }

    @Override
    public void onError(Throwable t) {
      // no-op
    }

    @Override
    public void onComplete() {
      // no-op
    }
  }
}
