package it.unitn.disi.ds1.message;

import java.io.Serializable;

/**
 * Client may timeout waiting for TXN begin confirmation (TxnAcceptMsg).
 */
public final class TxnAcceptTimeoutMsg implements Serializable {
    private static final long serialVersionUID = 5632463252006918229L;
}
