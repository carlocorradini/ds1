package it.unitn.disi.ds1.message.ops.write;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.message.TxnMsg;

import java.io.Serializable;
import java.util.UUID;

/**
 * Write request message
 * from {@link it.unitn.disi.ds1.actor.Client} to {@link it.unitn.disi.ds1.actor.Coordinator}.
 */
public final class WriteMessage extends TxnMsg implements Serializable {
    private static final long serialVersionUID = 8248714506636891726L;

    /**
     * Gson instance.
     */
    private static final Gson GSON = new GsonBuilder()
            .excludeFieldsWithoutExposeAnnotation()
            .create();

    /**
     * {@link it.unitn.disi.ds1.actor.Client} id
     */
    @Expose
    public final int clientId;

    /**
     * {@link it.unitn.disi.ds1.Item} key to write.
     */
    @Expose
    public final int key;

    /**
     * New {@link it.unitn.disi.ds1.Item} value to write.
     */
    @Expose
    public final int value;

    /**
     * Construct a new WriteMessage class.
     *
     * @param transactionId Transaction id
     * @param clientId      Client id
     * @param key           Item key to write
     * @param value         Item new value to write
     */
    public WriteMessage(UUID transactionId, int clientId, int key, int value) {
        super(transactionId);
        this.clientId = clientId;
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return GSON.toJson(this);
    }
}
