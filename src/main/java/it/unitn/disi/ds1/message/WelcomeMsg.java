package it.unitn.disi.ds1.message;

import akka.actor.ActorRef;
import it.unitn.disi.ds1.actor.Client;
import it.unitn.disi.ds1.actor.Coordinator;

import java.io.Serializable;
import java.util.*;

/**
 * Welcome message to {@link Client Client(s)} at startup to inform
 * about the available {@link Coordinator Coordinator(s)} and maximum key.
 */
public final class WelcomeMsg implements Serializable {
    private static final long serialVersionUID = 6825038584377226538L;

    /**
     * Available {@link Coordinator Coordinator(s)} in the actor system.
     */
    public final List<ActorRef> coordinators;

    /**
     * Maximum {@link it.unitn.disi.ds1.Item Item} key.
     */
    public final int maxItemKey;

    /**
     * Construct a welcome message with the available {@link Coordinator Coordinator(s)}
     * and maximum {@link it.unitn.disi.ds1.Item Item} key.
     *
     * @param coordinators Available {@link Coordinator Coordinator(s)} in the actor system
     * @param maxItemKey   Maximum {@link it.unitn.disi.ds1.Item Item} key
     */
    public WelcomeMsg(List<ActorRef> coordinators, int maxItemKey) {
        this.coordinators = List.copyOf(coordinators);
        this.maxItemKey = maxItemKey;
    }
}
