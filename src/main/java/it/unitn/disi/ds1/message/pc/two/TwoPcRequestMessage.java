package it.unitn.disi.ds1.message.pc.two;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.Serializable;
import java.util.UUID;

/**
 * Message from {@link it.unitn.disi.ds1.actor.Coordinator} to {@link it.unitn.disi.ds1.actor.DataStore}
 * asking if {@link it.unitn.disi.ds1.actor.DataStore} is able to commit or not
 */
public final class TwoPcRequestMessage extends TwoPcMessage implements Serializable {
    private static final long serialVersionUID = 6797846417399441318L;

    /**
     * Gson instance.
     */
    private static final Gson GSON = new GsonBuilder()
            .excludeFieldsWithoutExposeAnnotation()
            .create();

    /**
     * Construct a new RequestMessage class.
     *
     * @param transactionId Transaction id
     * @param decision      Decision
     */
    public TwoPcRequestMessage(UUID transactionId, boolean decision) {
        super(transactionId, decision);
    }

    @Override
    public String toString() {
        return GSON.toJson(this);
    }
}
