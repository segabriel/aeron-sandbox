package com.github.segabriel.cluster;

import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.Cluster.Role;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StubClusteredService implements ClusteredService {

  private static final Logger logger = LoggerFactory.getLogger(StubClusteredService.class);

  private final int id;
  private Cluster cluster;

  public StubClusteredService() {
    this.id = hashCode();
  }

  public StubClusteredService(int id) {
    this.id = id;
  }

  @Override
  public void onStart(Cluster cluster, Image snapshotImage) {
    this.cluster = cluster;
    logger.info(
        "id: {}, onStart => memberId: {}, role: {}, client-sessions: {}",
        id,
        cluster.memberId(),
        cluster.role(),
        cluster.clientSessions().size());
    if (snapshotImage != null) {
      logger.info(
          "id: {}, onLoadSnapshot => image: memberId: {}, sessionId: {}, channel: {}, "
              + "streamId: {}, position: {}",
          id,
          cluster.memberId(),
          snapshotImage.sessionId(),
          snapshotImage.subscription().channel(),
          snapshotImage.subscription().streamId(),
          snapshotImage.position());
    }
  }

  @Override
  public void onSessionOpen(ClientSession session, long timestampMs) {
    logger.info(
        "id: {}, onSessionOpen, timestampMs: {} => memberId: {}, sessionId: {}, "
            + "responseChannel: {}, responseStreamId: {}",
        id,
        timestampMs,
        cluster.memberId(),
        session.id(),
        session.responseChannel(),
        session.responseStreamId());
  }

  @Override
  public void onSessionClose(ClientSession session, long timestampMs, CloseReason closeReason) {
    logger.info(
        "id: {}, onSessionClose, timestampMs: {} => memberId: {}, "
            + "sessionId: {}, responseChannel: {}, responseStreamId: {}, reason: {}",
        id,
        timestampMs,
        cluster.memberId(),
        session.id(),
        session.responseChannel(),
        session.responseStreamId(),
        closeReason);
  }

  @Override
  public void onSessionMessage(
      ClientSession session,
      long timestampMs,
      DirectBuffer buffer,
      int offset,
      int length,
      Header header) {
    byte[] bytes = new byte[length];
    buffer.getBytes(offset, bytes);

    String message = new String(bytes);

    logger.info(
        "id: {}, onSessionMessage, timestampMs: {} => memberId: {}, "
            + "sessionId: {}, position: {}, content: '{}'",
        id,
        timestampMs,
        cluster.memberId(),
        session.id(),
        header.position(),
        message);
  }

  @Override
  public void onTimerEvent(long correlationId, long timestampMs) {
    logger.info(
        "id: {}, onTimerEvent, timestampMs: {} => memberId: {}, correlationId: {}",
        id,
        timestampMs,
        cluster.memberId(),
        correlationId);
  }

  @Override
  public void onTakeSnapshot(Publication snapshotPublication) {
    logger.info(
        "id: {}, onTakeSnapshot => publication: memberId: {}, sessionId: {}, channel: {}, "
            + "streamId: {}, position: {}",
        id,
        cluster.memberId(),
        snapshotPublication.sessionId(),
        snapshotPublication.channel(),
        snapshotPublication.streamId(),
        snapshotPublication.position());
  }

  @Override
  public void onRoleChange(Role newRole) {
    logger.info(
        "id: {}, onRoleChange => memberId: {}, new role: {}", id, cluster.memberId(), newRole);
  }

  @Override
  public void onTerminate(Cluster cluster) {
    logger.info(
        "id: {}, onTerminate => memberId: {}, role: {}, client-sessions: {}",
        id,
        cluster.memberId(),
        cluster.role(),
        cluster.clientSessions().size());
  }
}
