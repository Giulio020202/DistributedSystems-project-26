package it.unitn.ds;
import java.io.Serializable;

public class ReplicaMessage implements Serializable {

  public static class ReadRequestMessage extends ReplicaMessage {
    public final int index;

    public ReadRequestMessage(int index) {
      this.index = index;
    }
  }

  public static class ReadReply extends ReplicaMessage {
    public final int replicaId;
    public final int index;
    public final int value;

    public ReadReply(int replicaId, int index, int value) {
      this.replicaId = replicaId;
      this.index = index;
      this.value = value;
    }
  }

  public static class WriteRequestMessage extends ReplicaMessage {
    public final int index;
    public final int value;

    public WriteRequestMessage(int index, int value) {
      this.index = index;
      this.value = value;
    }
  }

  public static class WriteReply extends ReplicaMessage {
    public final int replicaId;
    public final int index;
    public final int value;

    public WriteReply(int replicaId, int index, int value) {
      this.replicaId = replicaId;
      this.index = index;
      this.value = value;
    }
  }

  public static class Heartbeat extends ReplicaMessage {
    public final int coordId;

    public Heartbeat(int coordId) {
      this.coordId = coordId;
    }
  }
}
