package it.unitn.disi.ds1.message.twopc;

import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;

/**
 * Message for {@link it.unitn.disi.ds1.actor.Coordinator} or {@link it.unitn.disi.ds1.actor.DataStore}
 * for communicating a timeout.
 */
public class TwoPcTimeoutMessage implements Serializable {
    private static final long serialVersionUID = 3387272272729304297L;

    /**
     * Construct a new TwoPcTimeoutMessage class.
     */
    public TwoPcTimeoutMessage() { }

    @Override
    public String toString() { return JsonUtil.GSON.toJson(this); }
}
