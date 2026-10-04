package it.unitn.ds;

import akka.actor.ActorRef;
import java.io.Serializable;
import java.util.*;

public abstract class ReplicaMessage implements Serializable {

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
    public final ActorRef sender;
    public final int index;
    public final int value;

    public WriteRequest(ActorRef sender, int index, int value) {
      this.sender = sender;
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

    public UpdateAck toAck() {
      return new UpdateAck(timestamp);
    }

    public CommitUpdate toCommit() {
      return new CommitUpdate(timestamp);
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

    public CoordinatorAnnouncement toCoordinator(){
      Map.Entry<Integer, LogicalTimestamp> coordinator_entry = null;

      // Iterate over every Replica-Timestamp association and save the most recent (new coordinator)
      for(var entry: replica_to_timestamp.entrySet()){
        if (coordinator_entry == null)
                coordinator_entry = entry;
        else{
          if(entry.getValue().compareTo(coordinator_entry.getValue()) > 0){
            coordinator_entry = entry;
          }
        }
      }

      return new CoordinatorAnnouncement(coordinator_entry.getKey(),replica_to_timestamp.keySet());
    }
  }

  public static class ElectionAck extends ReplicaMessage {}

  public static class CoordinatorAnnouncement extends ReplicaMessage {
    public final int new_coordinator_id;
    public final SortedSet<Integer> live_group;

    public CoordinatorAnnouncement(int new_coordinator_id, Set<Integer> live_group){
      this.new_coordinator_id = new_coordinator_id;
      this.live_group = Collections.unmodifiableSortedSet(new TreeSet<>(live_group));
    }
  }

  public static class Sync extends ReplicaMessage {
    public final int coordinator_id;
    public final LogicalTimestamp timestamp;
    public final Map<Integer, Integer> database_state;

    public Sync(int coordinator_id, LogicalTimestamp timestamp, Map<Integer, Integer> database_state) {
      this.coordinator_id = coordinator_id;
      this.timestamp = timestamp;
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
