package it.unitn.disi.ds1.exception;

/**
 * Transaction is running exception.
 */
public final class TransactionIsRunningException extends Exception {
    private static final long serialVersionUID = 3803381472615927868L;

    /**
     * Construct a new TransactionIsRunningException class.
     *
     * @param message Error message
     */
    public TransactionIsRunningException(String message) {
        super(message);
    }
}
