package it.unitn.disi.ds1.message.twopc;

import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;

/**
 * Message for {@link it.unitn.disi.ds1.actor.Coordinator} or {@link it.unitn.disi.ds1.actor.DataStore}
 * for recovering the state after a crash.
 */
public final class TwoPcRecoveryMessage implements Serializable {
    private static final long serialVersionUID = 8654000592527300706L;

    /**
     * Construct a new TwoPcRecoveryMessage class.
     */
    public TwoPcRecoveryMessage() { }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
