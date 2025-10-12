package chromahub.rhythm.app.data

import android.os.Parcelable
import kotlinx.parcelize.Parcelize

@Parcelize
data class LyricsData(
    val plainLyrics: String?,
    val syncedLyrics: String?,
    val wordByWordLyrics: String? = null // JSON string of Apple Music word-by-word lyrics
) : Parcelable {
    
    /**
     * Check if any lyrics are available
     */
    fun hasLyrics(): Boolean = !plainLyrics.isNullOrBlank() || !syncedLyrics.isNullOrBlank() || !wordByWordLyrics.isNullOrBlank()
    
    /**
     * Check if synced lyrics are available (either word-by-word or line-by-line)
     */
    fun hasSyncedLyrics(): Boolean = !syncedLyrics.isNullOrBlank() || !wordByWordLyrics.isNullOrBlank()
    
    /**
     * Check if word-by-word lyrics are available
     */
    fun hasWordByWordLyrics(): Boolean = !wordByWordLyrics.isNullOrBlank()
    
    /**
     * Get the best available lyrics for display, prioritizing synced then plain
     * Note: Word-by-word lyrics should be accessed via getWordByWordLyricsOrNull() and rendered separately
     */
    fun getBestLyrics(): String? = when {
        !syncedLyrics.isNullOrBlank() -> syncedLyrics
        !plainLyrics.isNullOrBlank() -> plainLyrics
        else -> null
    }
    
    /**
     * Get word-by-word lyrics if available, otherwise null
     */
    fun getWordByWordLyricsOrNull(): String? = wordByWordLyrics?.takeIf { it.isNotBlank() }
    
    /**
     * Get synced lyrics if available, otherwise null
     */
    fun getSyncedLyricsOrNull(): String? = syncedLyrics?.takeIf { it.isNotBlank() }
    
    /**
     * Get plain lyrics if available, otherwise null
     */
    fun getPlainLyricsOrNull(): String? = plainLyrics?.takeIf { it.isNotBlank() }
    
    /**
     * Check if the lyrics contain error messages
     */
    fun isErrorMessage(): Boolean {
        val errorMessages = listOf(
            "No lyrics found for this song",
            "Error fetching lyrics",
            "Lyrics not available offline",
            "No lyrics available for this song",
            "Unable to load lyrics. Tap to retry.",
            "Online-only lyrics enabled.\nConnect to the internet to view lyrics."
        )
        return errorMessages.any { error ->
            plainLyrics?.contains(error) == true || syncedLyrics?.contains(error) == true
        }
    }
}
