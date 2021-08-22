package it.unitn.disi.ds1.etc;

/**
 * Decision class.
 */
public enum Decision {
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
     * Construct a new Decision class.
     *
     * @param decision Decision
     */
    Decision(boolean decision) {
        this.decision = decision;
    }

    /**
     * Return {@link Decision} given a boolean decision value.
     *
     * @param decision Boolean decision
     * @return Decision instance
     */
    public static Decision valueOf(boolean decision) {
        return decision ? COMMIT : ABORT;
    }

    @Override
    public String toString() {
        if (decision) return "COMMIT";
        else return "ABORT";
    }
}
