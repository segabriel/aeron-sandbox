package com.github.segabriel.aeron.javaagent;

import static io.aeron.agent.EventConfiguration.EVENT_READER_FRAME_LIMIT;
import static io.aeron.agent.EventConfiguration.EVENT_RING_BUFFER;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.agent.DriverEventCode;
import io.aeron.agent.EventConfiguration;
import io.aeron.agent.EventLogAgent;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import java.lang.instrument.Instrumentation;
import net.bytebuddy.agent.ByteBuddyAgent;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;

public class Test {

  private static final String NETWORK_CHANNEL = "aeron:ipc?endpoint=localhost:54325";
  private static final int STREAM_ID = 777;

  public static void main(String[] args) throws InterruptedException {
    System.setProperty(EventConfiguration.ENABLED_EVENT_CODES_PROP_NAME, "all");
    System.setProperty(EventConfiguration.ENABLED_CLUSTER_EVENT_CODES_PROP_NAME, "all");
    System.setProperty(EventConfiguration.ENABLED_ARCHIVE_EVENT_CODES_PROP_NAME, "all");
    System.setProperty(EventLogAgent.READER_CLASSNAME_PROP_NAME, StubEventLogReaderAgent.class.getName());
    Instrumentation instrumentation = ByteBuddyAgent.install();
    EventLogAgent.agentmain("", instrumentation);

    try {



      final MediaDriver.Context driverCtx = new MediaDriver.Context()
          .errorHandler(Throwable::printStackTrace);

      try (MediaDriver ignore = MediaDriver.launchEmbedded(driverCtx))
      {
        final Aeron.Context clientCtx = new Aeron.Context()
            .aeronDirectoryName(driverCtx.aeronDirectoryName());

        try (Aeron aeron = Aeron.connect(clientCtx);
            Subscription subscription = aeron.addSubscription(NETWORK_CHANNEL, STREAM_ID);
            Publication publication = aeron.addPublication(NETWORK_CHANNEL, STREAM_ID))
        {
          final UnsafeBuffer offerBuffer = new UnsafeBuffer(new byte[32]);
          while (publication.offer(offerBuffer) < 0)
          {
            Thread.yield();
          }

          final MutableInteger counter = new MutableInteger();
          final FragmentHandler handler = (buffer, offset, length, header) -> counter.value++;

          while (0 == subscription.poll(handler, 1))
          {
            Thread.yield();
          }

        }
      }
      finally
      {
        driverCtx.deleteAeronDirectory();
      }

      Thread.sleep(2000);

    } finally{
      EventLogAgent.removeTransformer();
      System.clearProperty(EventConfiguration.ENABLED_EVENT_CODES_PROP_NAME);
      System.clearProperty(EventConfiguration.ENABLED_CLUSTER_EVENT_CODES_PROP_NAME);
      System.clearProperty(EventConfiguration.ENABLED_ARCHIVE_EVENT_CODES_PROP_NAME);
      System.clearProperty(EventLogAgent.READER_CLASSNAME_PROP_NAME);
    }
  }

  public static class StubEventLogReaderAgent implements Agent, MessageHandler
  {
    public String roleName()
    {
      return "event-log-reader";
    }

    public int doWork()
    {
      return EVENT_RING_BUFFER.read(this, EVENT_READER_FRAME_LIMIT);
    }

    public void onMessage(final int msgTypeId, final MutableDirectBuffer buffer, final int index, final int length)
    {
      System.out.println("msgTypeId = " + msgTypeId);
      System.out.println(buffer.getStringAscii(index, length));
    }
  }
}
