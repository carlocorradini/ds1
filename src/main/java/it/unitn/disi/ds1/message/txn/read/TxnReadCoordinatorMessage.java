package it.unitn.disi.ds1.message.txn.read;

import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.etc.Item;
import it.unitn.disi.ds1.message.txn.TxnMessage;
import it.unitn.disi.ds1.util.JsonUtil;

import java.util.UUID;

import java.io.Serializable;

/**
 * Read message
 * from {@link it.unitn.disi.ds1.actor.Coordinator} to {@link it.unitn.disi.ds1.actor.DataStore}.
 */
public final class TxnReadCoordinatorMessage extends TxnMessage implements Serializable {
    private static final long serialVersionUID = 4166460770428474735L;

    /**
     * {@link Item} key to read.
     */
    @Expose
    public final int key;

    /**
     * Construct a new TxnReadCoordinatorMessage class.
     *
     * @param coordinatorId {@link it.unitn.disi.ds1.actor.Coordinator} id
     * @param transactionId Transaction id
     * @param key           Item key to read
     */
    public TxnReadCoordinatorMessage(int coordinatorId, UUID transactionId, int key) {
        super(coordinatorId, transactionId);
        this.key = key;
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
