package it.unitn.disi.ds1.message;

import com.google.gson.annotations.Expose;

import java.io.Serializable;

/**
 * General abstract message.
 */
public abstract class Message implements Serializable {
    private static final long serialVersionUID = 301190222834691562L;

    /**
     * No {@link it.unitn.disi.ds1.actor.Actor Sender} id.
     */
    public static final int NO_SENDER_ID = -1;

    /**
     * {@link it.unitn.disi.ds1.actor.Actor Sender} id.
     */
    @Expose
    public final int senderId;

    /**
     * Construct a new Message class.
     *
     * @param senderId {@link it.unitn.disi.ds1.actor.Actor Sender} id.
     */
    public Message(int senderId) {
        this.senderId = senderId;
    }
}
