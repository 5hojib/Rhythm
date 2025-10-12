package chromahub.rhythm.app.network

import com.google.gson.annotations.SerializedName

/**
 * Apple Music song search result
 */
data class AppleMusicSearchResult(
    @SerializedName("id") val id: String,
    @SerializedName("songName") val songName: String?,
    @SerializedName("artistName") val artistName: String?,
    @SerializedName("albumName") val albumName: String?,
    @SerializedName("artwork") val artwork: String?,
    @SerializedName("releaseDate") val releaseDate: String?,
    @SerializedName("duration") val duration: Long?,
    @SerializedName("isrc") val isrc: String?,
    @SerializedName("url") val url: String?,
    @SerializedName("contentRating") val contentRating: String?,
    @SerializedName("albumId") val albumId: String?
)

/**
 * Apple Music lyrics response containing word-by-word synchronized lyrics
 */
data class AppleMusicLyricsResponse(
    @SerializedName("info") val info: String?,
    @SerializedName("type") val type: String?, // "Syllable" for word-by-word
    @SerializedName("content") val content: List<AppleMusicLyricsLine>?,
    @SerializedName("ttml_content") val ttmlContent: String?,
    @SerializedName("source") val source: String?,
    @SerializedName("track") val track: AppleMusicTrackInfo?
)

/**
 * Represents a line of lyrics with word-level synchronization
 */
data class AppleMusicLyricsLine(
    @SerializedName("text") val text: List<AppleMusicWord>?,
    @SerializedName("background") val background: Boolean?,
    @SerializedName("backgroundText") val backgroundText: List<String>?,
    @SerializedName("oppositeTurn") val oppositeTurn: Boolean?,
    @SerializedName("timestamp") val timestamp: Long?, // Line start timestamp in milliseconds
    @SerializedName("endtime") val endtime: Long? // Line end timestamp in milliseconds
)

/**
 * Represents a single word or syllable with precise timing
 */
data class AppleMusicWord(
    @SerializedName("text") val text: String,
    @SerializedName("part") val part: Boolean?, // true if this is part of a split word (syllable)
    @SerializedName("timestamp") val timestamp: Long, // Word start timestamp in milliseconds
    @SerializedName("endtime") val endtime: Long // Word end timestamp in milliseconds
)

/**
 * Track information from Apple Music
 */
data class AppleMusicTrackInfo(
    @SerializedName("albumName") val albumName: String?,
    @SerializedName("artistName") val artistName: String?,
    @SerializedName("name") val name: String?,
    @SerializedName("releaseDate") val releaseDate: String?,
    @SerializedName("hasLyrics") val hasLyrics: Boolean?,
    @SerializedName("hasTimeSyncedLyrics") val hasTimeSyncedLyrics: Boolean?
)
