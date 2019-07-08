package com.github.segabriel.cluster;

import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StubEgressListener implements EgressListener {

  private static final Logger logger = LoggerFactory.getLogger(StubEgressListener.class);

  @Override
  public void onMessage(
      long clusterSessionId,
      long timestampMs,
      DirectBuffer buffer,
      int offset,
      int length,
      Header header) {

    byte[] bytes = new byte[length];
    buffer.getBytes(offset, bytes);

    String message = new String(bytes);

    logger.info(
        "onMessage, timestampMs: {} => clusterSessionId:{},  position: {}, content: '{}'",
        timestampMs,
        clusterSessionId,
        header.position(),
        message);
  }

  @Override
  public void sessionEvent(
      long correlationId,
      long clusterSessionId,
      long leadershipTermId,
      int leaderMemberId,
      EventCode code,
      String detail) {
    logger.info(
        "sessionEvent => correlationId: {}, clusterSessionId: {}, leadershipTermId: {}, "
            + "leaderMemberId: {}, code: {}, detail: {},",
        correlationId,
        clusterSessionId,
        leadershipTermId,
        leaderMemberId,
        code,
        detail);
  }

  @Override
  public void newLeader(
      long clusterSessionId, long leadershipTermId, int leaderMemberId, String memberEndpoints) {
    logger.info(
        "newLeader => clusterSessionId: {}, leadershipTermId: {}, "
            + "leaderMemberId: {}, memberEndpoints: {},",
        clusterSessionId,
        leadershipTermId,
        leaderMemberId,
        memberEndpoints);
  }
}
