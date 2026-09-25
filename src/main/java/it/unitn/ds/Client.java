package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import akka.actor.Cancellable;

public class Client extends AbstractClient {

    Client(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, Optional<ActorRef> listener) {
        super(readTimeoutDelay, writeTimeoutDelay, listener, defaultTargetReplica);
    }

    public static Props props(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica) {
        return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, ActorRef listener) {
        return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.ofNullable(listener)));
    }

    // ASSUMPTION: NO MORE THAN ONE REQUEST IN FLIGHT
    private Cancellable currentTimeout = null;

    @Override
    public void sendRead(ActorRef replica, int index) {
        // Send Request to Replica
        replica.tell(new ReplicaMessage.ReadRequest(index), getSelf());
        // Schedule Read Timeout
        currentTimeout = getContext().system().scheduler().scheduleOnce(
            Duration.of(getReadTimeoutDelay(), ChronoUnit.MILLIS),
            getSelf(),
            new ReadTimeout(getSelf(), replica, index),
            getContext().system().dispatcher(),
            getSelf()
        );
    }

    @Override
    public void sendWrite(ActorRef replica, int index, int value) {
        // Send Request to Replica
        replica.tell(new ReplicaMessage.WriteRequest(index, value), getSelf());
        // Schedule Write Timeout
        currentTimeout = getContext().system().scheduler().scheduleOnce(
            Duration.of(getWriteTimeoutDelay(), ChronoUnit.MILLIS),
            getSelf(),
            new WriteTimeout(getSelf(), replica, index, value),
            getContext().system().dispatcher(),
            getSelf()
        );
    }

    public void onReadReply(ReplicaMessage.ReadReply msg) {
        // Cancel the scheduled timeout message
        currentTimeout.cancel();
        // Call callback
        callbackOnReadResult(new ReadResult(true, msg.index, msg.value, msg.replicaId));
    }

    public void onWriteReply(ReplicaMessage.WriteReply msg) {
        // Cancel the scheduled timeout message
        currentTimeout.cancel();
        // Call callback
        callbackOnWriteResult(new WriteResult(true, msg.index, msg.value, msg.replicaId));
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(ReadTimeout.class, this::callbackOnReadTimeout)
                .match(WriteTimeout.class, this::callbackOnWriteTimeout)
                .match(ReplicaMessage.ReadReply.class, this::onReadReply)
                .match(ReplicaMessage.WriteReply.class, this::onWriteReply)
                .build();
    }

}
