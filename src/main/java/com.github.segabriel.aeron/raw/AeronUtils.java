package com.github.segabriel.aeron.raw;

import io.aeron.Publication;
import io.aeron.logbuffer.BufferClaim;
import java.nio.ByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class AeronUtils {

  public static long send(Publication publication, ByteBuffer msgBody) {
    int msgLength = msgBody.remaining();
    int position = msgBody.position();
    int limit = msgBody.limit();

    if (msgLength < publication.maxPayloadLength()) {
      BufferClaim bufferClaim = new BufferClaim();
      long result = publication.tryClaim(msgLength, bufferClaim);
      if (result > 0) {
        try {
          MutableDirectBuffer dstBuffer = bufferClaim.buffer();
          int index = bufferClaim.offset();
          dstBuffer.putBytes(index, msgBody, position, limit);
          bufferClaim.commit();
        } catch (Exception ex) {
          bufferClaim.abort();
        }
      }
      return result;
    } else {
      return publication.offer(new UnsafeBuffer(msgBody, position, limit));
    }
  }
}
