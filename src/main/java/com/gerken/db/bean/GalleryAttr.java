package com.gerken.db.bean;

import java.util.Objects;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * A string indicating the subject matter or context of the images in a gallery.
 * It can be used to automatically prioritize crawl navigation.
 *
 * <p>Each attribute has optional {@code good} and {@code bad} flags that express a
 * bias for or against galleries bearing this attribute.</p>
 *
 * <p>Instances are created either via the primary-key constructor or by deserialising
 * a {@link JSONObject} (e.g. from a backup file). The primary-key field {@code name}
 * is immutable after construction.</p>
 */
public class GalleryAttr {

    /** The attribute keyword itself. Primary key; immutable. */
    private final String  name;

    /** Whether there is a bias in favor of galleries with this attribute. */
    private Boolean good;

    /** Whether there is a bias against galleries with this attribute. */
    private Boolean bad;

    /**
     * Constructs a GalleryAttr with the given primary key.
     * All non-PK fields are left {@code null} and should be populated via setters.
     *
     * @param name the attribute keyword (primary key); must not be {@code null}
     */
    public GalleryAttr(String name) {
        this.name = name;
    }

    /**
     * Constructs a GalleryAttr from a JSON object.
     * The {@code name} field is required; all other fields are optional and remain
     * {@code null} if absent or explicitly null in the JSON.
     *
     * @param json the JSON object containing gallery attribute data
     * @throws JSONException if the required {@code name} field is missing
     */
    public GalleryAttr(JSONObject json) throws JSONException {
        this.name = json.getString("name");
        if (json.has("good") && !json.isNull("good")) this.good = json.getBoolean("good");
        if (json.has("bad")  && !json.isNull("bad"))  this.bad  = json.getBoolean("bad");
    }

    /**
     * Returns the attribute keyword (primary key).
     *
     * @return the attribute name; never {@code null}
     */
    public String  getName() { return name; }

    /**
     * Returns whether there is a bias in favor of galleries with this attribute.
     *
     * @return {@code true} if there is a favorable bias, {@code false} if not, or {@code null} if unset
     */
    public Boolean getGood() { return good; }

    /**
     * Returns whether there is a bias against galleries with this attribute.
     *
     * @return {@code true} if there is a negative bias, {@code false} if not, or {@code null} if unset
     */
    public Boolean getBad()  { return bad; }

    /**
     * Sets whether there is a bias in favor of galleries with this attribute.
     *
     * @param good {@code true} if there is a favorable bias, {@code false} if not, or {@code null} to clear
     */
    public void setGood(Boolean good) { this.good = good; }

    /**
     * Sets whether there is a bias against galleries with this attribute.
     *
     * @param bad {@code true} if there is a negative bias, {@code false} if not, or {@code null} to clear
     */
    public void setBad(Boolean bad)   { this.bad  = bad; }

    /**
     * Two GalleryAttr instances are equal when their {@code name} (primary key) is equal.
     *
     * @param o the object to compare
     * @return {@code true} if {@code o} is a GalleryAttr with the same name
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GalleryAttr)) return false;
        GalleryAttr other = (GalleryAttr) o;
        return Objects.equals(name, other.name);
    }

    /**
     * Hash code based on the primary key {@code name}.
     *
     * @return hash code
     */
    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    /**
     * Returns a human-readable representation listing all fields in model-declaration order.
     *
     * @return string representation
     */
    @Override
    public String toString() {
        return "GalleryAttr[name=" + name + ", good=" + good + ", bad=" + bad + "]";
    }

    /**
     * Serialises this GalleryAttr to a JSON object.
     * Every field is included; Java {@code null} values are represented as
     * {@link JSONObject#NULL}.
     *
     * @return a JSONObject containing all fields
     * @throws JSONException if serialisation fails
     */
    public JSONObject asJson() throws JSONException {
        JSONObject json = new JSONObject();
        json.put("name", name);
        json.put("good", good == null ? JSONObject.NULL : good);
        json.put("bad",  bad  == null ? JSONObject.NULL : bad);
        return json;
    }
}
