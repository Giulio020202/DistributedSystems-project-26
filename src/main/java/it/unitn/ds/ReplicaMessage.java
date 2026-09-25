package it.unitn.ds;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;

public class ReplicaMessage implements Serializable {

  public static class ReadRequest extends ReplicaMessage {
    public final int index;

    public ReadRequest(int index) {
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

  public static class WriteRequest extends ReplicaMessage {
    public final int index;
    public final int value;

    public WriteRequest(int index, int value) {
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

  public static class Update extends ReplicaMessage {
    public final LogicalTimestamp timestamp;
    public final int index;
    public final int value;

    public Update(LogicalTimestamp timestamp, int index, int value) {
      this.timestamp = timestamp;
      this.index = index;
      this.value = value;
    }

    public UpdateAck getAck() {
      return new UpdateAck(timestamp);
    }
  }

  public static class UpdateAck extends ReplicaMessage {
    public final LogicalTimestamp timestamp;

    public UpdateAck(LogicalTimestamp timestamp) {
      this.timestamp = timestamp;
    }
  }

  public static class CommitUpdate extends ReplicaMessage {
    public final LogicalTimestamp timestamp;

    public CommitUpdate(LogicalTimestamp timestamp) {
      this.timestamp = timestamp;
    }
  }

  public static class Election extends ReplicaMessage {
    public final Map<Integer, LogicalTimestamp> replica_to_timestamp;

    public Election(Map<Integer, LogicalTimestamp> replica_to_timestamp) {
      this.replica_to_timestamp = Collections.unmodifiableMap(new HashMap<>(replica_to_timestamp));
    }
  }

  public static class ElectionAck extends ReplicaMessage {}

  public static class CoordinatorAnnouncement extends ReplicaMessage {
    public final int new_coordinator_id;

    public CoordinatorAnnouncement(int new_coordinator_id){
      this.new_coordinator_id = new_coordinator_id;
    }
  }

  public static class Sync extends ReplicaMessage {
    public final int coordinator_id;
    public final Map<Integer, Integer> database_state;

    public Sync(int coordinator_id, Map<Integer, Integer> database_state) {
      this.coordinator_id = coordinator_id;
      this.database_state = Collections.unmodifiableMap(new HashMap<>(database_state));
    }
  }

  public static class Heartbeat extends ReplicaMessage {
    public final int coordId;

    public Heartbeat(int coordId) {
      this.coordId = coordId;
    }
  }
}
