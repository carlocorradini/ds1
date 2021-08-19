package it.unitn.disi.ds1.message.ops.read;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.message.TxnMessage;

import java.io.Serializable;
import java.util.UUID;

/**
 * Read request message
 * from {@link it.unitn.disi.ds1.actor.Client} to {@link it.unitn.disi.ds1.actor.Coordinator}.
 */
public final class ReadMessage extends TxnMessage implements Serializable {
    private static final long serialVersionUID = 278859909154339067L;

    /**
     * Gson instance.
     */
    private static final Gson GSON = new GsonBuilder()
            .excludeFieldsWithoutExposeAnnotation()
            .create();

    /**
     * {@link it.unitn.disi.ds1.actor.Client} id.
     */
    @Expose
    public final int clientId;

    /**
     * {@link it.unitn.disi.ds1.Item} key to read.
     */
    @Expose
    public final int key;

    /**
     * Construct a new ReadMessage class.
     *
     * @param transactionId Transaction id
     * @param clientId      Client id
     * @param key           Item key to read
     */
    public ReadMessage(UUID transactionId, int clientId, int key) {
        super(transactionId);
        this.clientId = clientId;
        this.key = key;
    }

    @Override
    public String toString() {
        return GSON.toJson(this);
    }
}
