package com.gerken.db.bean;

import java.util.Objects;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * An image in a gallery, combining image-page and image-file metadata.
 *
 * <p>Instances are created either via the primary-key constructor or by deserialising
 * a {@link JSONObject} (e.g. from a backup file). The primary-key field {@code pageId}
 * is immutable after construction.</p>
 */
public class Image {

    /** The image page id. Primary key; immutable. */
    private final String  pageId;

    /** The gallery id this image belongs to. */
    private String  galleryId;

    /** The URL of the image page. */
    private String  pageUrl;

    /** The image id. Should be the same as the id of the image page that displays the image. */
    private String  imageId;

    /** The URL of the image file. */
    private String  imageUrl;

    /** The local filename (not the absolute path) of the downloaded image. */
    private String  filename;

    /**
     * A user-provided ranking of the image.
     * Values: unscored = -1, poor = 0, ok = 1, good = 2.
     */
    private Integer score;

    /**
     * Process state for crawl. One of:
     * GP (image discovered on gallery page),
     * IP (information gathered from image page),
     * DL (image downloaded).
     */
    private String state;

    /**
     * Constructs an Image with the given primary key.
     * All non-PK fields are left {@code null} and should be populated via setters.
     *
     * @param pageId the image page id (primary key); must not be {@code null}
     */
    public Image(String pageId) {
        this.pageId = pageId;
    }

    /**
     * Constructs an Image from a JSON object.
     * The {@code pageId} field is required; all other fields are optional and remain
     * {@code null} if absent or explicitly null in the JSON.
     *
     * @param json the JSON object containing image data
     * @throws JSONException if the required {@code pageId} field is missing
     */
    public Image(JSONObject json) throws JSONException {
        this.pageId = json.getString("pageId");
        if (json.has("galleryId") && !json.isNull("galleryId")) this.galleryId = json.getString("galleryId");
        if (json.has("pageUrl")   && !json.isNull("pageUrl"))   this.pageUrl   = json.getString("pageUrl");
        if (json.has("imageId")   && !json.isNull("imageId"))   this.imageId   = json.getString("imageId");
        if (json.has("imageUrl")  && !json.isNull("imageUrl"))  this.imageUrl  = json.getString("imageUrl");
        if (json.has("filename")  && !json.isNull("filename"))  this.filename  = json.getString("filename");
        if (json.has("score")     && !json.isNull("score"))     this.score     = json.getInt("score");
        if (json.has("state")     && !json.isNull("state"))     this.state     = json.getString("state");
    }

    /**
     * Returns the image page id (primary key).
     *
     * @return the page id; never {@code null}
     */
    public String  getPageId()    { return pageId; }

    /**
     * Returns the gallery id this image belongs to.
     *
     * @return the gallery id, or {@code null} if not set
     */
    public String  getGalleryId() { return galleryId; }

    /**
     * Returns the URL of the image page.
     *
     * @return the page URL, or {@code null} if not set
     */
    public String  getPageUrl()   { return pageUrl; }

    /**
     * Returns the image id. Should be the same as the id of the image page that displays the image.
     *
     * @return the image id, or {@code null} if not set
     */
    public String  getImageId()   { return imageId; }

    /**
     * Returns the URL of the image file.
     *
     * @return the image URL, or {@code null} if not set
     */
    public String  getImageUrl()  { return imageUrl; }

    /**
     * Returns the local filename (not the absolute path) of the downloaded image.
     *
     * @return the filename, or {@code null} if not set
     */
    public String  getFilename()  { return filename; }

    /**
     * Returns the user-provided ranking of this image.
     * Values: unscored = -1, poor = 0, ok = 1, good = 2.
     *
     * @return the score, or {@code null} if not set
     */
    public Integer getScore()     { return score; }

    /**
     * Returns the crawl process state:
     * GP (image discovered on gallery page),
     * IP (information gathered from image page), or
     * DL (image downloaded).
     *
     * @return the state value, or {@code null} if not set
     */
    public String getState()      { return state; }

    /**
     * Sets the gallery id this image belongs to.
     *
     * @param galleryId the gallery id, or {@code null} to clear
     */
    public void setGalleryId(String galleryId) { this.galleryId = galleryId; }

    /**
     * Sets the URL of the image page.
     *
     * @param pageUrl the page URL, or {@code null} to clear
     */
    public void setPageUrl(String pageUrl)     { this.pageUrl   = pageUrl; }

    /**
     * Sets the image id. Should be the same as the id of the image page that displays the image.
     *
     * @param imageId the image id, or {@code null} to clear
     */
    public void setImageId(String imageId)     { this.imageId   = imageId; }

    /**
     * Sets the URL of the image file.
     *
     * @param imageUrl the image URL, or {@code null} to clear
     */
    public void setImageUrl(String imageUrl)   { this.imageUrl  = imageUrl; }

    /**
     * Sets the local filename (not the absolute path) of the downloaded image.
     *
     * @param filename the filename, or {@code null} to clear
     */
    public void setFilename(String filename)   { this.filename  = filename; }

    /**
     * Sets the user-provided ranking of this image.
     * Values: unscored = -1, poor = 0, ok = 1, good = 2.
     *
     * @param score the score, or {@code null} to clear
     */
    public void setScore(Integer score)        { this.score     = score; }

    /**
     * Sets the crawl process state (GP, IP, or DL).
     *
     * @param state the state value, or {@code null} to clear
     * @see #getState()
     */
    public void setState(String state)         { this.state     = state; }

    /**
     * Two Image instances are equal when their {@code pageId} (primary key) is equal.
     *
     * @param o the object to compare
     * @return {@code true} if {@code o} is an Image with the same pageId
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Image)) return false;
        Image other = (Image) o;
        return Objects.equals(pageId, other.pageId);
    }

    /**
     * Hash code based on the primary key {@code pageId}.
     *
     * @return hash code
     */
    @Override
    public int hashCode() {
        return Objects.hash(pageId);
    }

    /**
     * Returns a human-readable representation listing all fields in model-declaration order.
     *
     * @return string representation
     */
    @Override
    public String toString() {
        return "Image[pageId=" + pageId + ", galleryId=" + galleryId +
               ", pageUrl=" + pageUrl + ", imageId=" + imageId +
               ", imageUrl=" + imageUrl + ", filename=" + filename +
               ", score=" + score + ", state=" + state + "]";
    }

    /**
     * Serialises this Image to a JSON object.
     * Every field is included; Java {@code null} values are represented as
     * {@link JSONObject#NULL}.
     *
     * @return a JSONObject containing all fields
     * @throws JSONException if serialisation fails
     */
    public JSONObject asJson() throws JSONException {
        JSONObject json = new JSONObject();
        json.put("pageId",    pageId);
        json.put("galleryId", galleryId == null ? JSONObject.NULL : galleryId);
        json.put("pageUrl",   pageUrl   == null ? JSONObject.NULL : pageUrl);
        json.put("imageId",   imageId   == null ? JSONObject.NULL : imageId);
        json.put("imageUrl",  imageUrl  == null ? JSONObject.NULL : imageUrl);
        json.put("filename",  filename  == null ? JSONObject.NULL : filename);
        json.put("score",     score     == null ? JSONObject.NULL : score);
        json.put("state",     state     == null ? JSONObject.NULL : state);
        return json;
    }
}
