package it.unitn.disi.ds1;

import java.util.StringJoiner;
import java.util.UUID;

public final class WriteRequest {
    public final UUID transactionId;
    public final int key;
    public final int actualVersion;
    public final int newValue;

    public WriteRequest(UUID transactionId, int key, int actualVersion, int newValue) {
        this.transactionId = transactionId;
        this.key = key;
        this.actualVersion = actualVersion;
        this.newValue = newValue;
    }

    @Override
    public String toString() { return String.format("TransactionId: %s, Key: %d, Actual version: %d, New value: %d", transactionId, key, actualVersion, newValue); }
}
