package com.github.segabriel.agent;

import io.aeron.Publication;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentTerminationException;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.SignalType;

public final class ReactorPublicationAgent implements Agent {

  private static final Logger logger = LoggerFactory.getLogger(ReactorPublicationAgent.class);

  private static final AtomicReferenceFieldUpdater<ReactorPublicationAgent, PublisherProcessor[]>
      PUBLISHER_PROCESSORS =
          AtomicReferenceFieldUpdater.newUpdater(
              ReactorPublicationAgent.class, PublisherProcessor[].class, "publisherProcessors");

  private final Publication publication;

  private final Duration connectTimeout = Duration.ofSeconds(5);
  private final Duration backpressureTimeout = Duration.ofSeconds(5);
  private final Duration adminActionTimeout = Duration.ofSeconds(5);

  private volatile PublisherProcessor[] publisherProcessors = new PublisherProcessor[0];

  private volatile Throwable lastError;

  public ReactorPublicationAgent(Publication publication) {
    this.publication = Objects.requireNonNull(publication, "publication cannot be null");
  }

  @Override
  public void onStart() {
    // no-op
  }

  @Override
  public int doWork() throws Exception {
    PublisherProcessor[] oldArray = this.publisherProcessors;
    int result = 0;

    Exception ex = null;

    //noinspection ForLoopReplaceableByForEach
    for (int i = 0; i < oldArray.length; i++) {
      PublisherProcessor processor = oldArray[i];

      processor.request();

      Object buffer = processor.buffer;
      if (buffer == null) {
        continue;
      }

      long r = 0;

      try {
        r = processor.publish(buffer);
      } catch (Exception e) {
        ex = e;
      }

      if (r > 0) {
        result++;
        processor.reset();
        continue;
      }

      // Handle closed publication
      if (r == Publication.CLOSED) {
        logger.warn("aeron.Publication is CLOSED: {}", this);
        ex = new AgentTerminationException("aeron.Publication is CLOSED");
      }

      // Handle max position exceeded
      if (r == Publication.MAX_POSITION_EXCEEDED) {
        logger.warn("aeron.Publication received MAX_POSITION_EXCEEDED: {}", this);
        ex = new AgentTerminationException("aeron.Publication received MAX_POSITION_EXCEEDED");
      }

      // Handle failed connection
      if (r == Publication.NOT_CONNECTED) {
        if (processor.isTimeoutElapsed(connectTimeout)) {
          logger.warn(
              "aeron.Publication failed to resolve NOT_CONNECTED within {} ms, {}",
              connectTimeout.toMillis(),
              this);
          ex = new AgentTerminationException("Failed to resolve NOT_CONNECTED within timeout");
        }
      }

      // Handle backpressure
      if (r == Publication.BACK_PRESSURED) {
        if (processor.isTimeoutElapsed(backpressureTimeout)) {
          logger.warn(
              "aeron.Publication failed to resolve BACK_PRESSURED within {} ms, {}",
              backpressureTimeout.toMillis(),
              this);
          ex = new AgentTerminationException("Failed to resolve BACK_PRESSURED within timeout");
        }
      }

      // Handle admin action
      if (r == Publication.ADMIN_ACTION) {
        if (processor.isTimeoutElapsed(adminActionTimeout)) {
          logger.warn(
              "aeron.Publication failed to resolve ADMIN_ACTION within {} ms, {}",
              adminActionTimeout.toMillis(),
              this);
          ex = new AgentTerminationException("Failed to resolve ADMIN_ACTION within timeout");
        }
      }

      if (ex != null) {
        break;
      }
    }

    if (ex != null) {
      lastError = ex;
      throw ex;
    }

    return result;
  }

  @Override
  public void onClose() {
    CloseHelper.quietClose(publication);

    // dispose processors
    PublisherProcessor[] oldArray = this.publisherProcessors;
    this.publisherProcessors = new PublisherProcessor[0];
    for (PublisherProcessor processor : oldArray) {
      try {
        processor.cancel();
        processor.onParentError(
            Optional.ofNullable(lastError)
                .orElse(new AgentTerminationException("PublisherProcessor has been cancelled")));
      } catch (Exception ex) {
        // no-op
      }
    }
  }

  @Override
  public String roleName() {
    return ReactorPublicationAgent.class.getName() + ":" + publication.sessionId();
  }

  public <B> Mono<Void> publish(
      Publisher<B> publisher, DirectBufferHandler<? super B> bufferHandler) {
    return Mono.defer(
        () -> {
          PublisherProcessor<B> processor = new PublisherProcessor<>(bufferHandler, this);
          publisher.subscribe(processor);
          return processor.onDispose();
        });
  }

  public interface DirectBufferHandler<B> {

    int estimateLength(B buffer);

    DirectBuffer map(B buffer, int length);

    void dispose(B buffer);
  }

  private static class PublisherProcessor<B> extends BaseSubscriber<B> {

    private final DirectBufferHandler<? super B> bufferHandler;
    private final ReactorPublicationAgent parent;

    private long start;
    private boolean requested;

    private final MonoProcessor<Void> onDispose = MonoProcessor.create();

    private volatile B buffer;
    private volatile Throwable error;

    PublisherProcessor(
        DirectBufferHandler<? super B> bufferHandler, ReactorPublicationAgent parent) {
      this.bufferHandler = bufferHandler;
      this.parent = parent;
      addSelf();
    }

    private Mono<Void> onDispose() {
      return onDispose;
    }

    void request() {
      if (requested || isDisposed()) {
        return;
      }

      Subscription upstream = upstream();
      if (upstream != null) {
        requested = true;
        upstream.request(1);
      }
    }

    void onParentError(Throwable throwable) {
      resetBuffer();
      onDispose.onError(throwable);
    }

    void reset() {
      resetBuffer();

      if (isDisposed()) {
        removeSelf();
        if (error != null) {
          onDispose.onError(error);
        } else {
          onDispose.onComplete();
        }
        return;
      }

      start = 0;
      requested = false;
    }

    @Override
    protected void hookOnSubscribe(Subscription subscription) {
      // no-op
    }

    @Override
    protected void hookOnNext(B value) {
      if (buffer != null) {
        try {
          bufferHandler.dispose(value);
        } catch (Exception ex) {
          logger.warn("Failed to release buffer: {}", value, ex);
        }
        throw Exceptions.failWithOverflow(
            "PublisherProcessor is overrun by more signals than expected");
      }
      buffer = value;
    }

    @Override
    protected void hookOnError(Throwable throwable) {
      error = throwable;
    }

    @Override
    protected void hookFinally(SignalType type) {
      if (buffer == null) {
        reset();
      }
    }

    long publish(B buffer) {
      if (start == 0) {
        start = System.currentTimeMillis();
      }
      int length = bufferHandler.estimateLength(buffer);
      return parent.publication.offer(bufferHandler.map(buffer, length));
    }

    boolean isTimeoutElapsed(Duration timeout) {
      return System.currentTimeMillis() - start > timeout.toMillis();
    }

    private void resetBuffer() {
      B oldBuffer = buffer;
      buffer = null;
      if (oldBuffer != null) {
        try {
          bufferHandler.dispose(oldBuffer);
        } catch (Exception ex) {
          logger.warn("Failed to release buffer: {}", oldBuffer, ex);
        }
      }
    }

    private void addSelf() {
      PublisherProcessor[] oldArray;
      PublisherProcessor[] newArray;
      do {
        oldArray = parent.publisherProcessors;
        newArray = ArrayUtil.add(oldArray, this);
      } while (!PUBLISHER_PROCESSORS.compareAndSet(parent, oldArray, newArray));
    }

    private void removeSelf() {
      PublisherProcessor[] oldArray;
      PublisherProcessor[] newArray;
      do {
        oldArray = parent.publisherProcessors;
        newArray = ArrayUtil.remove(oldArray, this);
      } while (!PUBLISHER_PROCESSORS.compareAndSet(parent, oldArray, newArray));
    }
  }
}
