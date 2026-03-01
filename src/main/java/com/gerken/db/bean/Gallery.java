package com.gerken.db.bean;

import java.util.Objects;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * A collection of images posted by a user and linked to by some number of gallery pages.
 *
 * <p>Instances are created either via the primary-key constructor or by deserialising
 * a {@link JSONObject} (e.g. from a backup file). The primary-key field {@code id}
 * is immutable after construction.</p>
 */
public class Gallery {

    /** The gallery id. Primary key; immutable. */
    private final String  id;

    /** The poster's user name. */
    private String  poster;

    /** The gallery name. */
    private String  name;

    /** The URL of the gallery page. */
    private String  url;

    /** A pipe-separated list of gallery categories and tags. */
    private String  attrs;

    /** Number of images in the gallery. */
    private Integer images;

    /** Number of pages in the gallery. */
    private Integer pages;

    /** Number of gallery pages that have been processed. */
    private Integer pagesSeen;

    /** Image size indicator: S, M, L, or XL. */
    private String  quality;

    /** When this gallery was added to the site (or modified), in yyyy-mm-dd format. */
    private String  added;

    /**
     * Process state for crawl. One of:
     * ID (identified as new from gallery page results list),
     * CA (categorized after crawling its first gallery page),
     * NA (navigating through all of its gallery pages),
     * IN (inventoried after completing crawl of all gallery pages).
     */
    private String state;

    /**
     * Constructs a Gallery with the given primary key.
     * All non-PK fields are left {@code null} and should be populated via setters.
     *
     * @param id the gallery id (primary key); must not be {@code null}
     */
    public Gallery(String id) {
        this.id = id;
    }

    /**
     * Constructs a Gallery from a JSON object.
     * The {@code id} field is required; all other fields are optional and remain
     * {@code null} if absent or explicitly null in the JSON.
     *
     * @param json the JSON object containing gallery data
     * @throws JSONException if the required {@code id} field is missing
     */
    public Gallery(JSONObject json) throws JSONException {
        this.id = json.getString("id");
        if (json.has("poster")    && !json.isNull("poster"))    this.poster    = json.getString("poster");
        if (json.has("name")      && !json.isNull("name"))      this.name      = json.getString("name");
        if (json.has("url")       && !json.isNull("url"))       this.url       = json.getString("url");
        if (json.has("attrs")     && !json.isNull("attrs"))     this.attrs     = json.getString("attrs");
        if (json.has("images")    && !json.isNull("images"))    this.images    = json.getInt("images");
        if (json.has("pages")     && !json.isNull("pages"))     this.pages     = json.getInt("pages");
        if (json.has("pagesSeen") && !json.isNull("pagesSeen")) this.pagesSeen = json.getInt("pagesSeen");
        if (json.has("quality")   && !json.isNull("quality"))   this.quality   = json.getString("quality");
        if (json.has("added")     && !json.isNull("added"))     this.added     = json.getString("added");
        if (json.has("state")     && !json.isNull("state"))     this.state     = json.getString("state");
    }

    /**
     * Returns the gallery id (primary key).
     *
     * @return the gallery id; never {@code null}
     */
    public String  getId()        { return id; }

    /**
     * Returns the poster's user name.
     *
     * @return the poster name, or {@code null} if not set
     */
    public String  getPoster()    { return poster; }

    /**
     * Returns the gallery name.
     *
     * @return the gallery name, or {@code null} if not set
     */
    public String  getName()      { return name; }

    /**
     * Returns the URL of the gallery page.
     *
     * @return the URL, or {@code null} if not set
     */
    public String  getUrl()       { return url; }

    /**
     * Returns the pipe-separated list of gallery categories and tags.
     *
     * @return the attrs string, or {@code null} if not set
     */
    public String  getAttrs()     { return attrs; }

    /**
     * Returns the number of images in the gallery.
     *
     * @return image count, or {@code null} if not set
     */
    public Integer getImages()    { return images; }

    /**
     * Returns the number of pages in the gallery.
     *
     * @return page count, or {@code null} if not set
     */
    public Integer getPages()     { return pages; }

    /**
     * Returns the number of gallery pages that have been processed.
     *
     * @return pages-seen count, or {@code null} if not set
     */
    public Integer getPagesSeen() { return pagesSeen; }

    /**
     * Returns the image size indicator (S, M, L, or XL).
     *
     * @return the quality code, or {@code null} if not set
     */
    public String  getQuality()   { return quality; }

    /**
     * Returns when this gallery was added to the site (or modified), in yyyy-mm-dd format.
     *
     * @return the date string, or {@code null} if not set
     */
    public String  getAdded()     { return added; }

    /**
     * Returns the crawl process state:
     * ID (identified as new from gallery page results list),
     * CA (categorized after crawling its first gallery page),
     * NA (navigating through all of its gallery pages), or
     * IN (inventoried after completing crawl of all gallery pages).
     *
     * @return the state value, or {@code null} if not set
     */
    public String getState()      { return state; }

    /**
     * Sets the poster's user name.
     *
     * @param poster the poster name, or {@code null} to clear
     */
    public void setPoster(String poster)        { this.poster    = poster; }

    /**
     * Sets the gallery name.
     *
     * @param name the gallery name, or {@code null} to clear
     */
    public void setName(String name)            { this.name      = name; }

    /**
     * Sets the URL of the gallery page.
     *
     * @param url the URL, or {@code null} to clear
     */
    public void setUrl(String url)              { this.url       = url; }

    /**
     * Sets the pipe-separated list of gallery categories and tags.
     *
     * @param attrs the attrs string, or {@code null} to clear
     */
    public void setAttrs(String attrs)          { this.attrs     = attrs; }

    /**
     * Sets the number of images in the gallery.
     *
     * @param images image count, or {@code null} to clear
     */
    public void setImages(Integer images)       { this.images    = images; }

    /**
     * Sets the number of pages in the gallery.
     *
     * @param pages page count, or {@code null} to clear
     */
    public void setPages(Integer pages)         { this.pages     = pages; }

    /**
     * Sets the number of gallery pages that have been processed.
     *
     * @param pagesSeen pages-seen count, or {@code null} to clear
     */
    public void setPagesSeen(Integer pagesSeen) { this.pagesSeen = pagesSeen; }

    /**
     * Sets the image size indicator (S, M, L, or XL).
     *
     * @param quality the quality code, or {@code null} to clear
     */
    public void setQuality(String quality)      { this.quality   = quality; }

    /**
     * Sets when this gallery was added to the site (or modified), in yyyy-mm-dd format.
     *
     * @param added the date string, or {@code null} to clear
     */
    public void setAdded(String added)          { this.added     = added; }

    /**
     * Sets the crawl process state (ID, CA, NA, or IN).
     *
     * @param state the state value, or {@code null} to clear
     * @see #getState()
     */
    public void setState(String state)          { this.state     = state; }

    /**
     * Two Gallery instances are equal when their {@code id} (primary key) is equal.
     *
     * @param o the object to compare
     * @return {@code true} if {@code o} is a Gallery with the same id
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Gallery)) return false;
        Gallery other = (Gallery) o;
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
        return "Gallery[id=" + id + ", poster=" + poster + ", name=" + name +
               ", url=" + url + ", attrs=" + attrs + ", images=" + images +
               ", pages=" + pages + ", pagesSeen=" + pagesSeen +
               ", quality=" + quality + ", added=" + added + ", state=" + state + "]";
    }

    /**
     * Serialises this Gallery to a JSON object.
     * Every field is included; Java {@code null} values are represented as
     * {@link JSONObject#NULL}.
     *
     * @return a JSONObject containing all fields
     * @throws JSONException if serialisation fails
     */
    public JSONObject asJson() throws JSONException {
        JSONObject json = new JSONObject();
        json.put("id",        id);
        json.put("poster",    poster    == null ? JSONObject.NULL : poster);
        json.put("name",      name      == null ? JSONObject.NULL : name);
        json.put("url",       url       == null ? JSONObject.NULL : url);
        json.put("attrs",     attrs     == null ? JSONObject.NULL : attrs);
        json.put("images",    images    == null ? JSONObject.NULL : images);
        json.put("pages",     pages     == null ? JSONObject.NULL : pages);
        json.put("pagesSeen", pagesSeen == null ? JSONObject.NULL : pagesSeen);
        json.put("quality",   quality   == null ? JSONObject.NULL : quality);
        json.put("added",     added     == null ? JSONObject.NULL : added);
        json.put("state",     state     == null ? JSONObject.NULL : state);
        return json;
    }
}
