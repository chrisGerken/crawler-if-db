package com.gerken.db.bean;

import java.util.Objects;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * A caption to be associated with an image. An image may have multiple captions.
 *
 * <p>The {@code id} field is a unique long value representing roughly the creation
 * time as Java epoch milliseconds. Use
 * {@link com.gerken.db.persist.PersistCrawlerIf#nextCaptionId()} to obtain a
 * guaranteed-unique id before inserting a new Caption.</p>
 *
 * <p>Instances are created either via the primary-key constructor or by deserialising
 * a {@link JSONObject} (e.g. from a backup file). The primary-key field {@code id}
 * is immutable after construction.</p>
 */
public class Caption {

    /** The unique caption id. Primary key; immutable. */
    private final Long id;

    /** The image id this caption is associated with. May not be unique in this table. */
    private String imageId;

    /** The text of the caption. */
    private String caption;

    /**
     * Constructs a Caption with the given primary key.
     * All non-PK fields are left {@code null} and should be populated via setters.
     *
     * @param id the unique caption id (primary key); must not be {@code null}
     */
    public Caption(Long id) {
        this.id = id;
    }

    /**
     * Constructs a Caption from a JSON object.
     * The {@code id} field is required; all other fields are optional and remain
     * {@code null} if absent or explicitly null in the JSON.
     *
     * @param json the JSON object containing caption data
     * @throws JSONException if the required {@code id} field is missing
     */
    public Caption(JSONObject json) throws JSONException {
        this.id = json.getLong("id");
        if (json.has("imageId") && !json.isNull("imageId")) this.imageId = json.getString("imageId");
        if (json.has("caption") && !json.isNull("caption")) this.caption = json.getString("caption");
    }

    /**
     * Returns the unique caption id (primary key).
     *
     * @return the id; never {@code null}
     */
    public Long getId() { return id; }

    /**
     * Returns the image id this caption is associated with.
     *
     * @return the image id, or {@code null} if not set
     */
    public String getImageId() { return imageId; }

    /**
     * Returns the text of the caption.
     *
     * @return the caption text, or {@code null} if not set
     */
    public String getCaption() { return caption; }

    /**
     * Sets the image id this caption is associated with.
     *
     * @param imageId the image id, or {@code null} to clear
     */
    public void setImageId(String imageId) { this.imageId = imageId; }

    /**
     * Sets the text of the caption.
     *
     * @param caption the caption text, or {@code null} to clear
     */
    public void setCaption(String caption) { this.caption = caption; }

    /**
     * Two Caption instances are equal when their {@code id} (primary key) is equal.
     *
     * @param o the object to compare
     * @return {@code true} if {@code o} is a Caption with the same id
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Caption)) return false;
        Caption other = (Caption) o;
        return Objects.equals(id, other.id);
    }

    /**
     * Hash code based on the primary key {@code id}.
     *
     * @return hash code
     */
    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    /**
     * Returns a human-readable representation listing all fields in model-declaration order.
     *
     * @return string representation
     */
    @Override
    public String toString() {
        return "Caption[id=" + id + ", imageId=" + imageId + ", caption=" + caption + "]";
    }

    /**
     * Serialises this Caption to a JSON object.
     * Every field is included; Java {@code null} values are represented as
     * {@link JSONObject#NULL}.
     *
     * @return a JSONObject containing all fields
     * @throws JSONException if serialisation fails
     */
    public JSONObject asJson() throws JSONException {
        JSONObject json = new JSONObject();
        json.put("id",      id);
        json.put("imageId", imageId == null ? JSONObject.NULL : imageId);
        json.put("caption", caption == null ? JSONObject.NULL : caption);
        return json;
    }
}
