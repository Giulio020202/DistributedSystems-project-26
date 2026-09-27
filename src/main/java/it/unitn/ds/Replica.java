package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds.ReplicaMessage.*;

import java.util.Map;
import java.util.Optional;

public class Replica extends AbstractReplica {

  public Replica(int id) {
    this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL,
        Optional.empty());
  }

  // Defining variables that will be populated on InitSystem
  // Not final because its this replica view of the system and need to be modified during elections
  private Map<Integer, ActorRef> group;
  private int coordinator_id;
  private State actorState = null;
  private Crash.Type nextCrashingMsg = null;

  // Any state subclass implements an interface that is MOSTLY pure with respect to the Actor Context
  // Most Actor Context 
  abstract class State {
    abstract void stateStart();
    abstract void stateStop();
    abstract void onReadRequest(ReplicaMessage.ReadRequest message);
    abstract void onReadReply(ReplicaMessage.ReadReply message);
    abstract void onWriteRequest(ReplicaMessage.WriteRequest message);
    abstract void onWriteReply(ReplicaMessage.WriteReply message);
    abstract void onUpdate(ReplicaMessage.Update message);
    abstract void onUpdateAck(ReplicaMessage.UpdateAck message);
    abstract void onCommitUpdate(ReplicaMessage.CommitUpdate message);
    abstract void onElection(ReplicaMessage.Election message);
    abstract void onElectionAck(ReplicaMessage.ElectionAck message);
    abstract void onSync(ReplicaMessage.Sync message);
    abstract void onHeartbeat(ReplicaMessage.Heartbeat message);
  }

  // Transition to this state when crashing condition is satisfied, start dropping all messages
  // For our implementation Crash is the only state that needs .become
  class Crashed extends State {
    @Override 
    void stateStart(){
      final Receive crashedReceive = receiveBuilder()
        .matchAny(msg -> {})
        .build();
      getContext().become(crashedReceive);
    }

    @Override 
    void stateStop(){}

    @Override
    void onReadRequest(ReadRequest message){}
    @Override
    void onReadReply(ReadReply message){}
    @Override
    void onWriteRequest(WriteRequest message){}
    @Override
    void onWriteReply(WriteReply message){}
    @Override
    void onUpdate(Update message){}
    @Override
    void onUpdateAck(UpdateAck message){}
    @Override
    void onCommitUpdate(CommitUpdate message){}
    @Override
    void onElection(Election message){}
    @Override
    void onElectionAck(ElectionAck message){}
    @Override
    void onSync(Sync message){}
    @Override
    void onHeartbeat(Heartbeat message){}
  }

  // Can a replica be read only?
  class ReadOnlyReplica extends State {
    //TODO: stateStart must set a timer, if do not recieve heartbeat for a while go start elections
  }

  class Coordinator extends State {
    //TODO: stateStart must set a timer that sends heartbeats
  }

  // Transition to this state when detecting a coordinator crash OR when recieving elecionstarted message (??)
  class Electing extends State {}

  public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
    super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
  }

  // Helper function to temporarily pause the current state when transitioning
  private void transitionState(State newState) {
    if(actorState != null)
      actorState.stateStop();
    actorState = newState;
    actorState.stateStart();
  }

  void onReadRequest(ReplicaMessage.ReadRequest message){
    actorState.onReadRequest(message);
  }
  
  void onReadReply(ReplicaMessage.ReadReply message){
    actorState.onReadReply(message);
  }
  
  void onWriteRequest(ReplicaMessage.WriteRequest message){
    actorState.onWriteRequest(message);
  }
  
  void onWriteReply(ReplicaMessage.WriteReply message){
    actorState.onWriteReply(message);
  }
  
  void onUpdate(ReplicaMessage.Update message){
    actorState.onUpdate(message);
  }
  
  void onUpdateAck(ReplicaMessage.UpdateAck message){
    actorState.onUpdateAck(message);
  }
  
  void onCommitUpdate(ReplicaMessage.CommitUpdate message){
    actorState.onCommitUpdate(message);
  }
  
  void onElection(ReplicaMessage.Election message){
    actorState.onElection(message);
  }
  
  void onElectionAck(ReplicaMessage.ElectionAck message){
    actorState.onElectionAck(message);
  }
  
  void onSync(ReplicaMessage.Sync message){
    actorState.onSync(message);
  }
  
  public void onHeartbeat(ReplicaMessage.Heartbeat msg){
    // non fa un cazzo probabilmente (per ora)
    // actorState.onHeartbeat(msg);
  }

  public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
    return Props.create(Replica.class,
        () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
  }

  // Props method for automated tests
  public static Props propsWithListener(int id, int minLatency, int maxLatency, int coordinatorBeatInterval,
      ActorRef listener) {
    return Props.create(Replica.class,
        () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.ofNullable(listener)));
  }

  // Getter for number of actors in the system. Crashed actors do not stop technically SO do they count for this method?
  @Override
  public int getSystemNumberOfActors() {
    //TODO: is this enough? prob not
    return this.group.size();
  }

  @Override
  public void crash(AbstractReplica.Crash how_to_crash) {
    if (how_to_crash.type == Crash.Type.Now)
      transitionState(new Crashed());
    else
      //if not crashing now store in variable the type of crash to use in behavior
      nextCrashingMsg = how_to_crash.type;
  }

  @Override
  public void initSystem(InitSystem sysInit) {
    this.group = sysInit.group;
    this.coordinator_id = sysInit.coordinator_id;

    // Decide initial state (either working or coordinating)
    if (id == coordinator_id){    // Coordinator id is initially decided and passed by Main
      transitionState(new Coordinator());
    } else {
      transitionState(new ReadOnlyReplica());
    }
  }

  @Override
  public final Receive createReceive() {
    return createBaseReceiveBuilder()
      .match(ReplicaMessage.ReadRequest.class, this::onReadRequest)
      .match(ReplicaMessage.ReadReply.class, this::onReadReply)
      .match(ReplicaMessage.WriteRequest.class, this::onWriteRequest)
      .match(ReplicaMessage.WriteReply.class, this::onWriteReply)
      .match(ReplicaMessage.Update.class, this::onUpdate)
      .match(ReplicaMessage.UpdateAck.class, this::onUpdateAck)
      .match(ReplicaMessage.CommitUpdate.class, this::onCommitUpdate)
      .match(ReplicaMessage.Election.class, this::onElection)
      .match(ReplicaMessage.ElectionAck.class, this::onElectionAck)
      .match(ReplicaMessage.Sync.class, this::onSync)
      .match(ReplicaMessage.Heartbeat.class, this::onHeartbeat)
      .build();
  }

}
