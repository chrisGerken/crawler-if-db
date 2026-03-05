package com.gerken.db.bean;

import java.util.Objects;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * A string indicating the subject matter or context of the images in a gallery.
 * It can be used to automatically prioritize crawl navigation.
 *
 * <p>Each attribute carries a {@code score} indicating crawl bias ("+", "-", " ",
 * or "?" if not yet scored) and an optional {@code factor} that adjusts the strength
 * of that score relative to other attributes with the same score.</p>
 *
 * <p>Instances are created either via the primary-key constructor or by deserialising
 * a {@link JSONObject} (e.g. from a backup file). The primary-key field {@code name}
 * is immutable after construction.</p>
 */
public class GalleryAttr {

    /** The attribute keyword itself. Primary key; immutable. */
    private final String  name;

    /**
     * The crawl bias for galleries with this attribute.
     * One of "+" (bias in favor), "-" (bias against), " " (no bias), or "?" (not yet scored).
     */
    private String  score;

    /** An additive that strengthens the score for this attribute relative to others with the same score. */
    private Integer factor;

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
        if (json.has("score")  && !json.isNull("score"))  this.score  = json.getString("score");
        if (json.has("factor") && !json.isNull("factor")) this.factor = json.getInt("factor");
    }

    /**
     * Returns the attribute keyword (primary key).
     *
     * @return the attribute name; never {@code null}
     */
    public String  getName()   { return name; }

    /**
     * Returns the crawl bias score for this attribute.
     * One of "+", "-", " ", or "?" (not yet scored), or {@code null} if unset.
     *
     * @return the score string, or {@code null} if unset
     */
    public String  getScore()  { return score; }

    /**
     * Returns the factor that adjusts this attribute's score strength relative to other
     * attributes with the same score.
     *
     * @return the factor, or {@code null} if unset
     */
    public Integer getFactor() { return factor; }

    /**
     * Sets the crawl bias score for this attribute.
     *
     * @param score one of "+", "-", " ", "?", or {@code null} to clear
     */
    public void setScore(String score)   { this.score  = score; }

    /**
     * Sets the factor that adjusts this attribute's score strength.
     *
     * @param factor the factor value, or {@code null} to clear
     */
    public void setFactor(Integer factor) { this.factor = factor; }

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
        return "GalleryAttr[name=" + name + ", score=" + score + ", factor=" + factor + "]";
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
        json.put("name",   name);
        json.put("score",  score  == null ? JSONObject.NULL : score);
        json.put("factor", factor == null ? JSONObject.NULL : factor);
        return json;
    }
}
