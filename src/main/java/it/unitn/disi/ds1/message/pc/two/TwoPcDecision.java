package it.unitn.disi.ds1.message.pc.two;

/**
 * 2PC available decisions.
 */
public enum TwoPcDecision {
    /**
     * Commit.
     */
    COMMIT(true),
    /**
     * Abort.
     */
    ABORT(false);

    private final boolean decision;

    TwoPcDecision(boolean decision) {
        this.decision = decision;
    }

    @Override
    public String toString() {
        return Boolean.toString(decision);
    }
}
