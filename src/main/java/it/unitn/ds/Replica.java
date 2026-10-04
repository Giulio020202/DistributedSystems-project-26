package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.ds.ReplicaMessage.*;

import java.io.Serializable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.HashMap;
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
  private final int[] positions;
  private State actorState = null;
  private Crash.Type nextCrashingMsg = null;
  private LogicalTimestamp current_timestamp;

  // Any state subclass implements an interface that is MOSTLY pure with respect to the Actor Context
  // Most Actor Context 
  abstract static class State {
    abstract void stateStart();
    abstract void stateStop();
    abstract void onWriteRequest(WriteRequest message);
    abstract void onUpdate(Update message);
    abstract void onUpdateAck(UpdateAck message);
    abstract void onCommitUpdate(CommitUpdate message);
    abstract void onElection(Election message);
    abstract void onElectionAck(ElectionAck message);
    abstract void onCoordinatorAnnouncement(CoordinatorAnnouncement message);
    abstract void onSync(Sync message);
    abstract void onHeartbeat(Heartbeat message);
  }

  // Transition to this state when crashing condition is satisfied, start dropping all messages
  // For our implementation Crash is the only state that needs .become
  final State crashedState = new State() {
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
    void onWriteRequest(WriteRequest message){}
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
    void onCoordinatorAnnouncement(CoordinatorAnnouncement message){}
    @Override
    void onSync(Sync message){}
    @Override
    void onHeartbeat(Heartbeat message){}
  };

  // Readonly because only the coordinator gets write requests, but they do write when coordinator says so
  class ReadOnlyReplica extends State {
    private final int coordinator_id;
    private Cancellable heartbeat_timeout;
    private Cancellable coordinator_timeout;

    // ASSUMPTION: THERE IS NO MORE THAN ONE UPDATE IN FLIGHT
    private Update pending_update;

    // We don't need to separate general Coordinator timeout and Heartbeat timeout
    private final Serializable coordinator_timeout_message = new Serializable(){};

    ReadOnlyReplica(int coordinator_id) {
      this.coordinator_id = coordinator_id;
    }

    @Override
    void stateStart() {
      final Receive with_heartbeat_timeout_handler = receiveBuilder()
        // Call onCoordinatorTimeout if message.equals(coordinator_timeout_message)
        // Since we always self-send the same object/instance, the default equals should be enough
        .matchEquals(coordinator_timeout_message, this::onCoordinatorTimeout)
        .build();
      getContext().become(createReceive().orElse(with_heartbeat_timeout_handler));

      scheduleHeartbeatTimeout();
    }

    @Override
    void stateStop() {
      if(heartbeat_timeout != null)
        heartbeat_timeout.cancel();

      if(coordinator_timeout != null)
        coordinator_timeout.cancel();

      getContext().become(createReceive());
    }

    void scheduleHeartbeatTimeout() {
      heartbeat_timeout = getContext()
                            .getSystem()
                            .scheduler()
                            .scheduleOnce(
                                    Duration.of(getCoordinatorBeatInterval(), ChronoUnit.MILLIS),
                                    // Duration.of(getCoordinatorBeatInterval()*2, ChronoUnit.MILLIS),
                                    getSelf(),
                                    coordinator_timeout_message,
                                    getContext().system().dispatcher(),
                                    getSelf()
                            );
    }

    void scheduleCoordinatorTimeout() {
      if(coordinator_timeout != null)
        coordinator_timeout.cancel();

      coordinator_timeout = getContext()
                              .getSystem()
                              .scheduler()
                              .scheduleOnce(
                                      Duration.of(getMaxLatencyPlusTolerance(), ChronoUnit.MILLIS),
                                      getSelf(),
                                      coordinator_timeout_message,
                                      getContext().system().dispatcher(),
                                      getSelf()
                              );
    }

    void transitionToElecting() {
      if(pending_update != null)
         transitionState(new Electing(pending_update));
       else
         transitionState(new Electing());
    }

    @Override
    void onHeartbeat(Heartbeat message) {
      if(heartbeat_timeout != null)
        heartbeat_timeout.cancel();
      scheduleHeartbeatTimeout();
    }

    void onCoordinatorTimeout(Serializable message) {
      transitionToElecting();
      ((Electing) actorState).startElection();
    }

    @Override
    void onWriteRequest(WriteRequest message) {
      tell(message, group.get(coordinator_id));
      // We may receive Write Requests from different clients,
      // but we only take the last one
      scheduleCoordinatorTimeout();
    }

    @Override
    void onUpdate(Update message) {
      coordinator_timeout.cancel();
      pending_update = message;
      tell(pending_update.toAck(), group.get(coordinator_id));
      scheduleCoordinatorTimeout();
    }

    @Override
    void onUpdateAck(UpdateAck message) {
      throw new UnsupportedOperationException("ReadOnly Replica shouldn't be receiving 'UpdateAck' messages");
    }

    @Override
    void onCommitUpdate(CommitUpdate message) {
      coordinator_timeout.cancel();
      if(pending_update.timestamp.compareTo(message.timestamp) != 0){
        //TODO: Maybe throw error
        return;
      }
      positions[pending_update.index] = pending_update.value;
      current_timestamp = pending_update.timestamp;
      pending_update = null;
      callbackOnUpdateApplied(pending_update.index, pending_update.value);
    }

    @Override
    void onElection(Election message) {
      // Transition and then pass message over to new state
      // Store pending update, if we are the new coordinator then we will be passing that timestamp
      // and committing the update
      transitionToElecting();
      actorState.onElection(message);
    }

    @Override
    void onElectionAck(ElectionAck message) {
      throw new UnsupportedOperationException("ReadOnly Replica shouldn't be receiving 'ElectionAck' messages");
    }

    @Override
    void onCoordinatorAnnouncement(CoordinatorAnnouncement message) {
      throw new UnsupportedOperationException("ReadOnly Replica shouldn't be receiving 'CoordinatorAnnouncement' messages");
    }

    @Override
    void onSync(Sync message) {
      throw new UnsupportedOperationException("ReadOnly Replica shouldn't be receiving 'Sync' messages");
    }
  }

  class Coordinator extends State {
    private final Heartbeat heartbeat_message = new Heartbeat(id);
    private final Queue<WriteRequest> write_queue = new ArrayDeque<>();

    // INVARIANT: pending_update != null => we are waiting for acks for that pending update
    private Update pending_update;
    private ActorRef update_sender;
    private int acks_received;

    private Cancellable heartbeat_timer;

    @Override
    void stateStart() {
      // Schedule first Hearbeat
      scheduleNextHeartbeat();
    }

    @Override
    void stateStop() {
      if(heartbeat_timer != null)
        heartbeat_timer.cancel();
    }

    // Method for scheduling Heartbeats, sends to self a hearbeat message that gets processed by onHeartbeat
    private void scheduleNextHeartbeat(){
      heartbeat_timer =
              getContext()
              .getSystem()
              .scheduler()
              .scheduleOnce(
                      Duration.of(getCoordinatorBeatInterval(), ChronoUnit.MILLIS),
                      getSelf(),
                      heartbeat_message,
                      getContext().system().dispatcher(),
                      getSelf()
              );
    }

    void maybeNextUpdate() {
      // If there's already a pending write or if the queue is empty, then exit early
      if(pending_update != null || write_queue.isEmpty())
        return;

      // Remove Write Request from the queue
      final WriteRequest pending_write = write_queue.remove();

      // Create Update message with next timestamp
      pending_update = new Update(
        current_timestamp.increaseSequenceNumber(),
        pending_write.index,
        pending_write.value
      );

      // Store Client
      update_sender = pending_write.sender;

      // Reset ack count
      acks_received = 0;

      // Broadcast pending update
      broadcast(pending_update);
      //TODO: Maybe do replica crash detection
      // Doing replica crash detection requires setting up a timeout, and we can't move on to the
      // next write without either waiting for the timeout or having unreliable crash detection
      // Which still wouldn't be much of a problem if we still used the orignal node count for quorum
    }

    void maybeCommitUpdate() {
      //TODO: If replica crash detection is done, we should probably still use the original
      // node count for quorum. We should send an email to ask
      // If we have received less than n/2+1 acks, exit early
      if(acks_received < (getSystemNumberOfActors()/2 + 1))
        return;

      // Update local state
      positions[pending_update.index] = pending_update.value;
      current_timestamp = pending_update.timestamp;

      // Broadcast commit message
      final CommitUpdate commit_message = pending_update.toCommit();
      broadcast(commit_message);

      // Callback
      callbackOnUpdateApplied(pending_update.index, pending_update.value);

      // Send reply to client
      tell(
        new WriteReply(id, pending_update.index, positions[pending_update.index]),
        update_sender
      );

      // Signal no pending update
      pending_update = null;

      // Process next upate in queue
      maybeNextUpdate();
    }

    // Method that actually sends the heartbeats to all replicas and then schedules next heartbeat.
    // Didnt use scheduleWithFixedDelay to avoid possible queueing multiple heartbeats
    @Override
    void onHeartbeat(Heartbeat message){
      broadcast(heartbeat_message);
      scheduleNextHeartbeat();
    }

    @Override
    void onWriteRequest(WriteRequest message) {
      write_queue.add(message);
      maybeNextUpdate();
    }

    @Override
    void onUpdate(Update message) {
      throw new UnsupportedOperationException("Coordinator shouldn't receive 'Update' messages");
    }

    @Override
    void onUpdateAck(UpdateAck message) {
      acks_received += 1;
      // If the original WriteRequest is a Forward, then remember to read the original sender
      maybeCommitUpdate();
    }

    @Override
    void onCommitUpdate(CommitUpdate message) {
      throw new UnsupportedOperationException("Coordinator shouldn't receive 'CommitUpdate' messages");
    }

    @Override
    void onElection(Election message) {
      throw new UnsupportedOperationException("Coordinator shouldn't receive 'Election' messages");
    }

    @Override
    void onElectionAck(ElectionAck message) {
      throw new UnsupportedOperationException("Coordinator shouldn't receive 'ElectionAck' messages");
    }

    @Override
    void onCoordinatorAnnouncement(CoordinatorAnnouncement message) {
      throw new UnsupportedOperationException("Coordinator shouldn't receive 'CoordinatorAnnouncement' messages");
    }

    @Override
    void onSync(Sync message) {
      throw new UnsupportedOperationException("Coordinator shouldn't receive 'Sync' messages");
    }
  }

  // Transition to this state when detecting a coordinator crash OR when recieving elecionstarted message (??)
  class Electing extends State {
    final Queue<WriteRequest> writes_queue = new ArrayDeque<>();
    final Update pending_update;
    final LogicalTimestamp last_timestamp;

    Electing() {
      pending_update = null;
      last_timestamp = current_timestamp;
    }

    Electing(Update pending_update) {
      this.pending_update = pending_update;
      this.last_timestamp = pending_update.timestamp;
    }

    public void startElection(){
      //TODO: Start election -- create Election message and send it
    }
    
    @Override
    void onWriteRequest(WriteRequest message) {
      writes_queue.add(message);
      // Later, when transitioning back, the queue should be emptied by rescheduling the messages
      //TODO: maybe not queue? Writes can be lost already if the coordinator dies
      // while it has them in its queue, we should either save 'em all or discard them all
      // This way, the semantic of the timeout is solely dependent on its value
    }

    @Override
    void onUpdate(Update message) {
    throw new UnsupportedOperationException("Electing state shouldn't receive 'Update' messages");
    }

    @Override
    void onUpdateAck(UpdateAck message) {
    throw new UnsupportedOperationException("Electing state shouldn't receive 'UpdateAck' messages");
    }

    @Override
    void onCommitUpdate(CommitUpdate message) {
    throw new UnsupportedOperationException("Electing state shouldn't receive 'CommitUpdate' messages");
    }

  }

  public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
    super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
    this.positions = new int[POSITIONS_LIST_LENGTH];
  }

  // Helper function to temporarily pause the current state when transitioning
  private void transitionState(State newState) {
    if(actorState != null) // Initially all nodes actorState are initialized as null
      actorState.stateStop();
    actorState = newState;
    actorState.stateStart();
  }

  void onReadRequest(ReadRequest message){
    if(message.index < positions.length)
      tell(
        new ReadReply(id, message.index, positions[message.index]),
        getSender()
      );
    else {
      //TODO: Maybe error?
    }
  }
  
  void onWriteRequest(WriteRequest message){
    actorState.onWriteRequest(message);
  }
  
  void onUpdate(Update message){
    actorState.onUpdate(message);

    if(nextCrashingMsg == Crash.Type.Update)
      transitionState(crashedState);
  }
  
  void onUpdateAck(UpdateAck message){
    actorState.onUpdateAck(message);
  }
  
  void onCommitUpdate(CommitUpdate message){
    actorState.onCommitUpdate(message);

    if(nextCrashingMsg == Crash.Type.WriteOK)
      transitionState(crashedState);
  }
  
  void onElection(Election message){
    actorState.onElection(message);

    if(nextCrashingMsg == Crash.Type.Election)
      transitionState(crashedState);
  }
  
  void onElectionAck(ElectionAck message){
    actorState.onElectionAck(message);

    if(nextCrashingMsg == Crash.Type.Election)
      transitionState(crashedState);

  }

  void onCoordinatorAnnouncement(CoordinatorAnnouncement message){
    actorState.onCoordinatorAnnouncement(message);

    if(nextCrashingMsg == Crash.Type.Election)
      transitionState(crashedState);
  }
  
  void onSync(Sync message){
    actorState.onSync(message);

    // Assuming that Synchronization is election-related (ends election)
    if(nextCrashingMsg == Crash.Type.Election)
      transitionState(crashedState);
  }
  
  public void onHeartbeat(Heartbeat msg){
    actorState.onHeartbeat(msg);
    if(nextCrashingMsg == Crash.Type.Heartbeat)
      transitionState(crashedState);
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

  // Getter for number of live actors
  @Override
  public int getSystemNumberOfActors() {
    return this.group.size();
  }

  @Override
  public void crash(AbstractReplica.Crash how_to_crash) {
    if (how_to_crash.type == Crash.Type.Now)
      transitionState(crashedState);
    else
      //if not crashing now store in variable the type of crash to use in behavior
      nextCrashingMsg = how_to_crash.type;
  }

  @Override
  public void initSystem(InitSystem sysInit) {
    this.group = sysInit.group;

    for(int i = 0; i < POSITIONS_LIST_LENGTH; i++){
      positions[i] = 0;
    }
    this.current_timestamp = LogicalTimestamp.ZERO;

    // Decide initial state (either working or coordinating)
    if (id == sysInit.coordinator_id){    // Coordinator id is initially decided and passed by Main
      transitionState(new Coordinator());
    } else {
      transitionState(new ReadOnlyReplica(sysInit.coordinator_id));
    }
  }

  void broadcast(Serializable message){
    for(Map.Entry<Integer, ActorRef> entry: group.entrySet()) {
      if(entry.getKey() != id)
        tell(message, entry.getValue());
    }
  }

  @Override
  public final Receive createReceive() {
    return createBaseReceiveBuilder()
      .match(ReadRequest.class, this::onReadRequest)
      .match(WriteRequest.class, this::onWriteRequest)
      .match(Update.class, this::onUpdate)
      .match(UpdateAck.class, this::onUpdateAck)
      .match(CommitUpdate.class, this::onCommitUpdate)
      .match(Election.class, this::onElection)
      .match(ElectionAck.class, this::onElectionAck)
      .match(CoordinatorAnnouncement.class, this::onCoordinatorAnnouncement)
      .match(Sync.class, this::onSync)
      .match(Heartbeat.class, this::onHeartbeat)
      .build();
  }
}
