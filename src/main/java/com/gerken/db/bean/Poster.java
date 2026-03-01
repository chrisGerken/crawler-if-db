package com.gerken.db.bean;

import java.util.Objects;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * The owner and producer of one or more galleries.
 *
 * <p>Instances are created either via the primary-key constructor or by deserialising
 * a {@link JSONObject} (e.g. from a backup file). The primary-key field {@code name}
 * is immutable after construction.</p>
 */
public class Poster {

    /** The poster's user id. Primary key; immutable. */
    private final String name;

    /** The URL of the primary page of the poster's galleries. */
    private String url;

    /** W (whitelisted), B (blacklisted) or G (no decision yet, default) indicating crawl priorities for newly posted galleries. */
    private String color;

    /**
     * Constructs a Poster with the given primary key.
     * All non-PK fields are left {@code null} and should be populated via setters.
     *
     * @param name the poster's user id (primary key); must not be {@code null}
     */
    public Poster(String name) {
        this.name = name;
    }

    /**
     * Constructs a Poster from a JSON object.
     * The {@code name} field is required; all other fields are optional and remain
     * {@code null} if absent or explicitly null in the JSON.
     *
     * @param json the JSON object containing poster data
     * @throws JSONException if the required {@code name} field is missing
     */
    public Poster(JSONObject json) throws JSONException {
        this.name = json.getString("name");
        if (json.has("url") && !json.isNull("url")) this.url = json.getString("url");
        if (json.has("color") && !json.isNull("color")) this.color = json.getString("color");
    }

    /**
     * Returns the poster's user id (primary key).
     *
     * @return the poster name; never {@code null}
     */
    public String getName()  { return name; }

    /**
     * Returns the URL of the primary page of the poster's galleries.
     *
     * @return the URL, or {@code null} if not set
     */
    public String getUrl()   { return url; }

    /**
     * Returns the crawl priority indicator for newly posted galleries:
     * W (whitelisted), B (blacklisted), or G (no decision yet, default).
     *
     * @return the colour code, or {@code null} if not set
     */
    public String getColor() { return color; }

    /**
     * Sets the URL of the primary page of the poster's galleries.
     *
     * @param url the URL, or {@code null} to clear
     */
    public void setUrl(String url)     { this.url = url; }

    /**
     * Sets the crawl priority indicator for newly posted galleries:
     * W (whitelisted), B (blacklisted), or G (no decision yet, default).
     *
     * @param color the colour code, or {@code null} to clear
     */
    public void setColor(String color) { this.color = color; }

    /**
     * Two Posters are equal when their {@code name} (primary key) is equal.
     *
     * @param o the object to compare
     * @return {@code true} if {@code o} is a Poster with the same name
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Poster)) return false;
        Poster other = (Poster) o;
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
        return "Poster[name=" + name + ", url=" + url + ", color=" + color + "]";
    }

    /**
     * Serialises this Poster to a JSON object.
     * Every field is included; Java {@code null} values are represented as
     * {@link JSONObject#NULL}.
     *
     * @return a JSONObject containing all fields
     * @throws JSONException if serialisation fails
     */
    public JSONObject asJson() throws JSONException {
        JSONObject json = new JSONObject();
        json.put("name",  name);
        json.put("url",   url   == null ? JSONObject.NULL : url);
        json.put("color", color == null ? JSONObject.NULL : color);
        return json;
    }
}
