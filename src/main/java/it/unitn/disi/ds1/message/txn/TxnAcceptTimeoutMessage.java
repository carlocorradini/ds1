package it.unitn.disi.ds1.message.txn;

import java.io.Serializable;

/**
 * Timeout message for {@link TxnBeginMessage} informing that no answer has been received.
 */
public final class TxnAcceptTimeoutMessage implements Serializable {
    private static final long serialVersionUID = 5632463252006918229L;
}
