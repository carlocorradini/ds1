package it.unitn.disi.ds1.message;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.List;

/**
 * Message to the client at startup to inform it about the coordinators and the keys.
 */
public final class WelcomeMsg implements Serializable {
    private static final long serialVersionUID = 6825038584377226538L;

    public final Integer maxKey;
    public final List<ActorRef> coordinators;

    public WelcomeMsg(int maxKey, List<ActorRef> coordinators) {
        this.maxKey = maxKey;
        this.coordinators = List.copyOf(coordinators);
    }
}
