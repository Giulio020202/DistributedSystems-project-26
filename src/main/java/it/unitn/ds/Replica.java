package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.Pair;

import java.util.Optional;

public class Replica extends AbstractReplica {

    private Pair<ActorRef,Integer> coordinator = new Pair<>;

    // States in which a node can be, useful later for changing behavior
    private AbstractReplica.Receive working;
    private AbstractReplica.Receive coordinating;
    private AbstractReplica.Receive electing;
    private AbstractReplica.Receive crashed;

    // Definition of states enum so that we can keep track of actor's state easily
    enum State{
        WORKING, COORDINATING, ELECTING, CRASHED
    }
    // For now on startup assume we are working
    // TODO: Check if this is the case, probably not (actor zero starts as coordinator)
    private State actorState = State.WORKING;

    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL, Optional.empty());
    }

    public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
        // TODO: implement
        // receiveBuilder is implemented so that nodes can behave like state machines
        // states are transitioned in createReceive with become
        // TODO: add all possible states and matching receive functions
        working = receiveBuilder()
                .match(ElectionStarted.class, this::receiveElectionStarted)
                .match(Crash.class, this::receiveCrash)
                .build();
        coordinating =  receiveBuilder()
                .match(Crash.class, this::receiveCrash)
                .build();
        electing = receiveBuilder()
                .match(CoordinatorElected.class, this::receiveCoordinatorElected)
                .match(Crash.class, this::receiveCrash)
                .build();
        crashed = receiveBuilder()
                .build();
    }

    // Functions that implement the logic when receiving specific type of message
    // These are called by public Replica when building the recieive builder
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
            msg.updates.put(id, ); // TODO: add current update
            do{
                tell(msg,);
            }while()
        }
        else if (actorState == State.ELECTING){
            // This branch means that the election has finished and we should send the coordinator elected message
        }
        else{
            System.err.println("Error: This branch should never be triggered");
            System.exit(1);
        }
    }

    private void receiveCoordinatorElected(CoordinatorElected msg){
        // Receiving this message means coordinator has been elected so we can go back to working state
        // This can be only electing -> working or also electing -> coordinator ???
    }

    private void receiveCrash(Crash crash_msg){
        if (crash_msg.type == Crash.Type.Now){
            // If crash now instantly transition to crashed state
            // TODO: implement state transition
        }else{
            //idk tbh
        }
    }

    public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, ActorRef listener) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.ofNullable(listener)));
    }

    @Override
    public int getSystemNumberOfActors() {
        // TODO: implement
        return 0;
    }

    @Override
    public void crash(AbstractReplica.Crash how_to_crash) {
        // TODO: implement
    }

    @Override
    public void initSystem(InitSystem sysInit) {
        // TODO: implement
    }


    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                // TODO add your message handlers here .match(, )
                // used to change  between states which are defined in public Replica
                .matchEquals("sos", s -> getContext().become(working))
                .matchEquals("sas", s -> getContext().become(coordinating))
                .matchEquals("ses", s -> getContext().become(electing))
                .matchEquals("sus", s -> getContext().become(crashed))
                .build();
    }

}
