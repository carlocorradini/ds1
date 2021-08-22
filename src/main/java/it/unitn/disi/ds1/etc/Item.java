package it.unitn.disi.ds1.etc;

import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.actor.DataStore;
import it.unitn.disi.ds1.util.JsonUtil;

import java.util.UUID;

/**
 * Simple item stored in a {@link DataStore}.
 */
public final class Item {
    /**
     * Available Item operations.
     */
    public enum Operation {
        /**
         * None.
         */
        NONE,
        /**
         * Read.
         */
        READ,
        /**
         * Write.
         */
        WRITE
    }

    /**
     * Value of the item.
     * Any change applied updates the version (version + 1).
     */
    @Expose
    public final int value;

    /**
     * Version of the item.
     * Any change applied to value also updates its version (version + 1).
     * A new item has version set to DEFAULT_VERSION.
     */
    @Expose
    public final int version;

    /**
     * Operation done on the Item.
     */
    @Expose
    public final Operation operation;

    /**
     * {@link UUID Transaction} that is locking the Item.
     */
    @Expose
    public volatile UUID locker;

    /**
     * Construct a new Item class.
     *
     * @param value     Value of the item
     * @param version   Version of the item
     * @param operation Operation done on the Item
     * @param locker    {@link UUID Transaction} that is locking the item
     */
    private Item(int value, int version, Operation operation, UUID locker) {
        this.value = value;
        this.version = version;
        this.operation = operation;
        this.locker = locker;
    }

    /**
     * Check if Item is locked by a {@link UUID locker}.
     *
     * @return True if locked, false otherwise.
     */
    public synchronized boolean isLocked() {
        return locker != null;
    }

    /**
     * Check if {@link UUID locker} is the current locker that is locking the Item.
     * Note that if Item is not locked by any locker the returned value is false.
     *
     * @param locker {@link UUID Locker} to check
     * @return True if same locker, false otherwise
     */
    public synchronized boolean isLocker(UUID locker) {
        return isLocked() && this.locker.equals(locker);
    }

    /**
     * Lock the Item by locker and return true if the operation has been successful.
     *
     * @param locker {@link UUID Locker} trying to lock the Item
     * @return True if locked, false otherwise
     */
    public synchronized boolean lock(UUID locker) {
        // Check if Item is locked and the locker is different
        if (isLocked() && !isLocker(locker)) return false;

        // Lock item
        this.locker = locker;
        return true;
    }

    /**
     * Remove the {@link UUID locker} that is locking the Item
     * only if the locker is the same.
     *
     * @param locker {@link UUID Locker} locking the Item
     */
    public synchronized void unlock(UUID locker) {
        if (isLocker(locker)) this.locker = null;
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }

    /**
     * {@link Item} builder class.
     */
    public static class Builder {
        private final int value;
        private final int version;
        private final UUID locker;
        private Operation operation;

        public Builder(int value, int version) {
            this.value = value;
            this.version = version;
            this.locker = null;
            this.operation = Operation.NONE;
        }

        public Builder withOperation(Operation operation) {
            this.operation = operation;
            return this;
        }

        public Item build() {
            return new Item(value, version, operation, locker);
        }
    }
}
