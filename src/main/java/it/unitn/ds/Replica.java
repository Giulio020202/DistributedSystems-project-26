package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.Pair;

import java.io.Serializable;
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
  private State actorState = State.WORKING;
  private Crash.Type nextCrashingMsg = null;

  // States in which a node can be, useful later for changing behavior
  private AbstractReplica.Receive working;
  private AbstractReplica.Receive coordinating;
  private AbstractReplica.Receive electing;
  private AbstractReplica.Receive crashed;

  // Definition of states enum so that we can keep track of actor's state easily
  enum State {
    WORKING, COORDINATING, ELECTING, CRASHED
  }

  public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
    super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
    // TODO: implement
    // receiveBuilder is implemented so that nodes can behave like state machines
    // states are transitioned in createReceive with become
    // TODO: add all possible states and matching receive functions

    working = receiveBuilder()
        .match(ElectionStarted.class, this::receiveElectionStarted)
        .build();
    coordinating = receiveBuilder()
        .build();
    electing = receiveBuilder()
        .match(CoordinatorElected.class, this::receiveCoordinatorElected)
        .build();
    crashed = receiveBuilder()
        // Match everything and do nothing
        .matchAny((msg)->{})
        .build();
  }

  // Functions that implement the logic when receiving specific type of message
  // These are called by public Replica when building the receive builder
  private void receiveElectionStarted(ElectionStarted msg){
        // Receiving this message means someone detected coordinator crash and started an election
        // Just need to change state to electing (?) and wait for the election process
        //TODO implement logic when election started
        if (actorState == State.WORKING){
            // This means it's the first time the actor receives a election started message
            // Transition into electing
            actorState =  State.ELECTING;
            getContext().become(electing);

            // Add this actor's update to the map and send msg to the next node in the circle
            msg.updates.put(id, this.); // TODO: add current update WE DO NOT HAVE REPLICA DATA YET
            do{
                tell(msg,);
            }while()
        }
        else if (actorState == State.ELECTING){
            // This branch means that the election has finished, and we should send the coordinator elected message
        }
        else{
            System.err.println("Error: This branch should never be triggered");
            System.exit(1);
        }
    }

  private void receiveCoordinatorElected(CoordinatorElected msg) {
    // Receiving this message means coordinator has been elected so we can go back
    // to working state
    // This can be only electing -> working or also electing -> coordinator ???
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

  @Override
  public int getSystemNumberOfActors() {
    // TODO: is this enough? prob not
    return this.group.size();
  }

  @Override
  public void crash(AbstractReplica.Crash how_to_crash) {


    if (how_to_crash.type == Crash.Type.Now)
        getContext().become(crashed);
    else
        //if not crashing now store in variable the type of crash to use in behavior
        nextCrashingMsg = how_to_crash.type;

  }

  @Override
  public void initSystem(InitSystem sysInit) {
    // TODO: did i do this right?
    this.group = sysInit.group;
    this.coordinator_id = sysInit.coordinator_id;

    // Decide initial state (either working or coordinating)
    if ( id == coordinator_id)
      actorState = State.COORDINATING;
      //TODO set a timer that sends heartbeats
    else{
      //TODO set a timer, if do not recieve heartbeat for a while go start elections
    }
  }


  @Override
  public final Receive createReceive() {
    return createBaseReceiveBuilder()
        // TODO add your message handlers here .match(, )
        // used to change between states which are defined in public Replica
        .matchEquals("sos", s -> getContext().become(working))
        .matchEquals("sas", s -> getContext().become(coordinating))
        .matchEquals("ses", s -> getContext().become(electing))
        .matchEquals("sus", s -> getContext().become(crashed))
        .build();
  }

  public static class ReadRequestMessage implements Serializable {
    public final int index;

    public ReadRequestMessage(int index) {
      this.index = index;
    }
  }

  public static class ReadReply implements Serializable {
    public final int replicaId;
    public final int index;
    public final int value;

    public ReadReply(int replicaId, int index, int value) {
      this.replicaId = replicaId;
      this.index = index;
      this.value = value;
    }
  }

  public static class WriteRequestMessage implements Serializable {
    public final int index;
    public final int value;

    public WriteRequestMessage(int index, int value) {
      this.index = index;
      this.value = value;
    }
  }

  public static class WriteReply implements Serializable {
    public final int replicaId;
    public final int index;
    public final int value;

    public WriteReply(int replicaId, int index, int value) {
      this.replicaId = replicaId;
      this.index = index;
      this.value = value;
    }
  }

  public static class Heartbeat implements Serializable{
    public final int coordId;

    public Heartbeat(int coordId) {
      this.coordId = coordId;
    }
  }

  public void onHeartbeat(Heartbeat msg){
    // non fa un cazzo probabilmente (per ora)
  }

}
