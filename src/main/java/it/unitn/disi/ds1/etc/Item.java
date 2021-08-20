package it.unitn.disi.ds1.etc;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.actor.DataStore;

import java.util.UUID;

/**
 * Simple item stored in a {@link DataStore}.
 */
public final class Item {
    /**
     * Default value.
     */
    public static final int DEFAULT_VALUE = 100;

    /**
     * Default version.
     */
    public static final int DEFAULT_VERSION = 0;

    /**
     * Default locker.
     */
    public static final UUID DEFAULT_LOCKER = null;

    /**
     * Gson instance.
     */
    private static final Gson GSON = new GsonBuilder()
            .excludeFieldsWithoutExposeAnnotation()
            .create();

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
     * {@link UUID Transaction} that is locking the Item.
     */
    @Expose
    public UUID locker;

    /**
     * Construct a new Item class.
     *
     * @param value   Value of the item
     * @param version Version of the item
     * @param locker  {@link UUID Transaction} that is locking the item
     */
    public Item(int value, int version, UUID locker) {
        this.value = value;
        this.version = version;
        this.locker = locker;
    }

    /**
     * Construct a new Item class.
     *
     * @param value   Value of the item
     * @param version Version of the item
     */
    public Item(int value, int version) {
        this(value, version, DEFAULT_LOCKER);
    }

    /**
     * Construct a new Item class.
     *
     * @param value Value of the item
     */
    public Item(int value) {
        this(value, DEFAULT_VERSION);
    }

    /**
     * Construct a new Item class.
     */
    public Item() {
        this(DEFAULT_VALUE);
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
        return isLocked() && this.locker == locker;
    }

    /**
     * Lock the Item by locker and return if the operation has been successful.
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
     * Remove the {@link UUID locker} that is locking the Item.
     */
    private synchronized void unlock() {
        locker = null;
    }

    /**
     * Remove the {@link UUID locker} that is locking the Item
     * only if the locker is the same.
     *
     * @param locker {@link UUID Locker} locking the Item
     */
    public synchronized void unlockIfIsLocker(UUID locker) {
        if (isLocker(locker)) unlock();
    }

    @Override
    public String toString() {
        return GSON.toJson(this);
    }
}
