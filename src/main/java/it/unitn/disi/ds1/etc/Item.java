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
     * Value of the Item.
     */
    @Expose
    private int value;

    /**
     * Version of the Item.
     */
    @Expose
    private int version;

    /**
     * {@link UUID Transaction} that is locking the Item.
     */
    @Expose
    private volatile UUID locker;

    /**
     * Boolean flag used to check if the value of
     * the Item has changed after creation.
     */
    private boolean valueChanged;

    /**
     * Construct a new Item class.
     *
     * @param value   Value of the item
     * @param version Version of the item
     */
    public Item(int value, int version) {
        this.value = value;
        this.version = version;
        this.locker = null;
        this.valueChanged = false;
    }

    /**
     * Return value of the Item.
     *
     * @return Item value
     */
    public int getValue() {
        return value;
    }

    /**
     * Return version of the Item.
     *
     * @return Item version
     */
    public int getVersion() {
        return version;
    }

    /**
     * Return {@link UUID locker} of the Item.
     *
     * @return Item locker
     */
    public UUID getLocker() {
        return locker;
    }

    /**
     * Set new value of the Item.
     *
     * @param value New Item value
     */
    public void setValue(int value) {
        this.value = value;

        if (!valueChanged) {
            valueChanged = true;
            version += 1;
        }
    }

    /**
     * Return true if the value of the Item has changed after creation, false otherwise.
     *
     * @return True if value changed, false otherwise.
     */
    public boolean isValueChanged() {
        return valueChanged;
    }

    /**
     * Check if Item is locked by a {@link UUID locker}.
     *
     * @return True if locked, false otherwise.
     */
    public boolean isLocked() {
        return locker != null;
    }

    /**
     * Check if {@link UUID locker} is the current locker that is locking the Item.
     * Note that if Item is not locked by any locker the returned value is false.
     *
     * @param locker {@link UUID Locker} to check
     * @return True if same locker, false otherwise
     */
    public boolean isLocker(UUID locker) {
        return isLocked() && this.locker.equals(locker);
    }

    /**
     * Lock the Item by locker and return true if the operation has been successful.
     *
     * @param locker {@link UUID Locker} trying to lock the Item
     * @return True if locked, false otherwise
     */
    public boolean lock(UUID locker) {
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
    public void unlock(UUID locker) {
        if (isLocker(locker)) this.locker = null;
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
