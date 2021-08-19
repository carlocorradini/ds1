package it.unitn.disi.ds1.message.twopc;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.Serializable;
import java.util.UUID;

/**
 * Reply from {@link it.unitn.disi.ds1.actor.DataStore} to {@link it.unitn.disi.ds1.actor.Coordinator}
 * with Yes or No decision for committing
 */

public final class ResponseMessage extends TwoPCMessage implements Serializable {
    private static final long serialVersionUID = 4917833122149828262L;

    /**
     * Gson instance.
     */
    private static final Gson GSON = new GsonBuilder()
            .excludeFieldsWithoutExposeAnnotation()
            .create();

    /**
     * Construct a new ResponseMessage class.
     *
     * @param transactionId Transaction id
     * @param  decision Decision
     */
    public ResponseMessage(UUID transactionId, boolean decision) {
        super(transactionId, decision);
    }

    @Override
    public String toString() {
        return GSON.toJson(this);
    }
}
