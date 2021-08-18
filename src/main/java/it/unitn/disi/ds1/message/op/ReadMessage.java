package it.unitn.disi.ds1.message.op;

import it.unitn.disi.ds1.message.TxnMsg;

import java.io.Serializable;
import java.util.UUID;

/**
 * Read request message
 * from {@link it.unitn.disi.ds1.actor.Client} to {@link it.unitn.disi.ds1.actor.Coordinator}.
 */
public final class ReadMessage extends TxnMsg implements Serializable {
    private static final long serialVersionUID = 278859909154339067L;

    /**
     * {@link it.unitn.disi.ds1.actor.Client} id.
     */
    public final int clientId;

    /**
     * {@link it.unitn.disi.ds1.Item} key to read.
     */
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
}
