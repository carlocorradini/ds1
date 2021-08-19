package it.unitn.disi.ds1.message.op.read;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.message.txn.TxnMessage;

import java.util.UUID;

import java.io.Serializable;

/**
 * Read request message
 * from {@link it.unitn.disi.ds1.actor.Coordinator} to {@link it.unitn.disi.ds1.actor.DataStore}.
 */
public final class ReadCoordinatorMessage extends TxnMessage implements Serializable {
    private static final long serialVersionUID = 4166460770428474735L;

    /**
     * Gson instance.
     */
    private static final Gson GSON = new GsonBuilder()
            .excludeFieldsWithoutExposeAnnotation()
            .create();

    /**
     * {@link it.unitn.disi.ds1.Item} key to read.
     */
    @Expose
    public final int key;

    /**
     * Construct a new ReadCoordinatorMessage class.
     *
     * @param transactionId Transaction id
     * @param key           Item key to read
     */
    public ReadCoordinatorMessage(UUID transactionId, int key) {
        super(transactionId);
        this.key = key;
    }

    @Override
    public String toString() {
        return GSON.toJson(this);
    }
}
