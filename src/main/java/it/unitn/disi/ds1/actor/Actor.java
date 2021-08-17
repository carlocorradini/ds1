package it.unitn.disi.ds1.actor;

import akka.actor.AbstractActor;

/**
 * General abstract actor.
 */
public abstract class Actor extends AbstractActor {
    /**
     * Actor identifier.
     */
    public final int id;

    /**
     * Construct an abstract actor.
     *
     * @param id Actor identifier
     */
    public Actor(int id) {
        this.id = id;
    }
}
