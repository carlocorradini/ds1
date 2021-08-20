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

    /**
     * Decision made.
     */
    private final boolean decision;

    /**
     * Construct a new TwoPcDecision class.
     *
     * @param decision Decision
     */
    TwoPcDecision(boolean decision) {
        this.decision = decision;
    }

    /**
     * Return true if decision is to COMMIT, false otherwise.
     *
     * @return True COMMIT, false otherwise
     */
    public boolean toBoolean() {
        return toBoolean(this);
    }

    /**
     * Return true if decision is to COMMIT, false otherwise.
     *
     * @param decision Decision
     * @return True COMMIT, false otherwise
     */
    public static boolean toBoolean(TwoPcDecision decision) {
        return decision == COMMIT;
    }

    /**
     * Return {@link TwoPcDecision} given a boolean decision value.
     *
     * @param decision Boolean decision
     * @return TwoPcDecision
     */
    public static TwoPcDecision valueOf(boolean decision) {
        return decision ? COMMIT : ABORT;
    }

    @Override
    public String toString() {
        if (decision) return "COMMIT";
        else return "ABORT";
    }
}
