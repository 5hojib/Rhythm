package chromahub.rhythm.app.data

import android.content.ContentUris
import android.content.Context
import android.net.Uri
import android.os.Build
import android.provider.MediaStore
import android.util.Log
import chromahub.rhythm.app.network.NetworkClient
import chromahub.rhythm.app.network.DeezerApiService
import chromahub.rhythm.app.network.DeezerArtist
import chromahub.rhythm.app.network.DeezerAlbum
import chromahub.rhythm.app.network.YTMusicApiService
import chromahub.rhythm.app.network.YTMusicSearchRequest
import chromahub.rhythm.app.network.YTMusicContext
import chromahub.rhythm.app.network.YTMusicClient
import chromahub.rhythm.app.network.YTMusicBrowseRequest
import chromahub.rhythm.app.network.extractArtistImageUrl
import chromahub.rhythm.app.network.extractAlbumImageUrl
import chromahub.rhythm.app.network.extractArtistBrowseId
import chromahub.rhythm.app.network.extractAlbumBrowseId
import chromahub.rhythm.app.network.extractArtistThumbnail
import chromahub.rhythm.app.network.extractAlbumCover
import okhttp3.Request
import com.google.gson.JsonParser
import com.google.gson.Gson
import java.net.URLEncoder
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.yield
import java.io.File
import java.net.URL
import chromahub.rhythm.app.data.LyricsData
import java.lang.ref.WeakReference

class MusicRepository(context: Context) {
    private val TAG = "MusicRepository"
    // Use WeakReference to prevent context leaks, but store applicationContext which is safe
    private val contextRef = WeakReference(context.applicationContext)
    private val context: Context
        get() = contextRef.get() ?: throw IllegalStateException("Context has been garbage collected")
    
    private val repositoryScope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    /**
     * API Fallback Strategy:
     * 
     * ARTIST IMAGES:
     * 1. Check local cache/storage
     * 2. Deezer API (primary)
     * 3. YouTube Music (fallback)
     * 4. Placeholder generation
     * 
     * ALBUM ARTWORK:
     * 1. Check existing local album art
     * 2. Check cache
     * 3. Deezer API (primary)
     * 4. YouTube Music (only when local album art is absent)
     * 5. Placeholder generation
     * 
     * TRACK IMAGES:
     * 1. Check if track already has artwork
     * 2. Check if album has artwork (inherit from album)
     * 3. YouTube Music (fallback when no local artwork available)
     */
    
    private val deezerApiService = NetworkClient.deezerApiService
    private val lrclibApiService = NetworkClient.lrclibApiService
    private val ytmusicApiService = NetworkClient.ytmusicApiService
    private val appleMusicApiService = NetworkClient.appleMusicApiService
    private val genericHttpClient = NetworkClient.genericHttpClient

    // LRU caches for artist images, album artwork, and lyrics to avoid memory leaks
    private val artistImageCache = object : LinkedHashMap<String, Uri?>(16, 0.75f, true) {
        override fun removeEldestEntry(eldest: Map.Entry<String, Uri?>?): Boolean {
            return size > MAX_ARTIST_CACHE_SIZE
        }
    }
    private val albumImageCache = object : LinkedHashMap<String, Uri?>(32, 0.75f, true) {
        override fun removeEldestEntry(eldest: Map.Entry<String, Uri?>?): Boolean {
            return size > MAX_ALBUM_CACHE_SIZE
        }
    }
    private val lyricsCache = object : LinkedHashMap<String, LyricsData>(50, 0.75f, true) {
        override fun removeEldestEntry(eldest: Map.Entry<String, LyricsData>?): Boolean {
            return size > MAX_LYRICS_CACHE_SIZE
        }
    }
    
    // Rate limiting for API calls
    private val lastApiCalls = mutableMapOf<String, Long>()
    private val apiCallCounts = mutableMapOf<String, Int>()
    
    companion object {
        private const val MAX_ARTIST_CACHE_SIZE = 100
        private const val MAX_ALBUM_CACHE_SIZE = 200
        private const val MAX_LYRICS_CACHE_SIZE = 150
        
        // API rate limiting constants
        private const val DEEZER_MIN_DELAY = 200L
        private const val YTMUSIC_MIN_DELAY = 300L
        private const val LRCLIB_MIN_DELAY = 100L
        private const val MAX_CALLS_PER_MINUTE = 30
    }
    
    private fun calculateApiDelay(apiName: String, currentTime: Long): Long {
        val lastCall = lastApiCalls[apiName] ?: 0L
        val minDelay = when (apiName.lowercase()) {
            "deezer" -> DEEZER_MIN_DELAY
            "ytmusic" -> YTMUSIC_MIN_DELAY
            "lrclib" -> LRCLIB_MIN_DELAY
            else -> 250L
        }
        
        val timeSinceLastCall = currentTime - lastCall
        if (timeSinceLastCall < minDelay) {
            return minDelay - timeSinceLastCall
        }
        
        // Check if we're making too many calls per minute
        val callsInLastMinute = apiCallCounts[apiName] ?: 0
        if (callsInLastMinute >= MAX_CALLS_PER_MINUTE) {
            // Exponential backoff
            return minDelay * 2
        }
        
        return 0L
    }
    
    private fun updateLastApiCall(apiName: String, timestamp: Long) {
        lastApiCalls[apiName] = timestamp
        
        // Update call count for rate limiting
        val currentCount = apiCallCounts[apiName] ?: 0
        apiCallCounts[apiName] = currentCount + 1
        
        // Reset counter every minute
        if (currentCount == 0) {
            repositoryScope.launch {
                delay(60000)
                apiCallCounts[apiName] = 0
            }
        }
    }

    suspend fun loadSongs(): List<Song> = withContext(Dispatchers.IO) {
        val startTime = System.currentTimeMillis()
        val songs = mutableListOf<Song>()
        val collection = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            MediaStore.Audio.Media.getContentUri(MediaStore.VOLUME_EXTERNAL)
        } else {
            MediaStore.Audio.Media.EXTERNAL_CONTENT_URI
        }

        val projection = arrayOf(
            MediaStore.Audio.Media._ID,
            MediaStore.Audio.Media.TITLE,
            MediaStore.Audio.Media.ARTIST,
            MediaStore.Audio.Media.ALBUM,
            MediaStore.Audio.Media.ALBUM_ID,
            MediaStore.Audio.Media.DURATION,
            MediaStore.Audio.Media.TRACK,
            MediaStore.Audio.Media.YEAR,
            MediaStore.Audio.Media.DATE_ADDED,
            MediaStore.Audio.Media.SIZE,
            MediaStore.Audio.Media.GENRE
        )

        // Improved selection to filter out very short files and invalid entries
        val selection = "${MediaStore.Audio.Media.IS_MUSIC} = 1 AND ${MediaStore.Audio.Media.DURATION} > 10000"
        val sortOrder = "${MediaStore.Audio.Media.DATE_ADDED} DESC"

        try {
            context.contentResolver.query(
                collection,
                projection,
                selection,
                null,
                sortOrder
            )?.use { cursor ->
                val count = cursor.count
                Log.d(TAG, "Found $count audio files to process")
                
                if (count == 0) {
                    Log.w(TAG, "No audio files found in MediaStore")
                    return@withContext emptyList()
                }

                // Pre-allocate list with known size for better performance
                if (songs is ArrayList) {
                    songs.ensureCapacity(count)
                }
                
                // Cache all column indices once
                val columnIndices = try {
                    ColumnIndices(
                        id = cursor.getColumnIndexOrThrow(MediaStore.Audio.Media._ID),
                        title = cursor.getColumnIndexOrThrow(MediaStore.Audio.Media.TITLE),
                        artist = cursor.getColumnIndexOrThrow(MediaStore.Audio.Media.ARTIST),
                        album = cursor.getColumnIndexOrThrow(MediaStore.Audio.Media.ALBUM),
                        albumId = cursor.getColumnIndexOrThrow(MediaStore.Audio.Media.ALBUM_ID),
                        duration = cursor.getColumnIndexOrThrow(MediaStore.Audio.Media.DURATION),
                        track = cursor.getColumnIndexOrThrow(MediaStore.Audio.Media.TRACK),
                        year = cursor.getColumnIndexOrThrow(MediaStore.Audio.Media.YEAR),
                        dateAdded = cursor.getColumnIndexOrThrow(MediaStore.Audio.Media.DATE_ADDED),
                        size = cursor.getColumnIndexOrThrow(MediaStore.Audio.Media.SIZE),
                        genre = cursor.getColumnIndexOrThrow(MediaStore.Audio.Media.GENRE),
                        albumArtist = cursor.getColumnIndex(MediaStore.Audio.Media.ALBUM_ARTIST) // May be -1 on older devices
                    )
                } catch (e: IllegalArgumentException) {
                    Log.e(TAG, "Required column not found in MediaStore", e)
                    return@withContext emptyList()
                }

                var processedCount = 0
                val batchSize = 1000
                
                while (cursor.moveToNext()) {
                    try {
                        val song = createSongFromCursor(cursor, columnIndices)
                        if (song != null) {
                            songs.add(song)
                        }
                        
                        processedCount++
                        // Yield control periodically to avoid blocking
                        if (processedCount % batchSize == 0) {
                            yield()
                        }
                    } catch (e: Exception) {
                        Log.w(TAG, "Error processing song at position ${cursor.position}", e)
                        continue
                    }
                }
                
                val endTime = System.currentTimeMillis()
                Log.d(TAG, "Loaded ${songs.size} songs in ${endTime - startTime}ms")
            }
        } catch (e: Exception) {
            Log.e(TAG, "Error querying MediaStore for songs", e)
            return@withContext emptyList()
        }

        return@withContext songs
    }
    
    private data class ColumnIndices(
        val id: Int,
        val title: Int,
        val artist: Int,
        val album: Int,
        val albumId: Int,
        val duration: Int,
        val track: Int,
        val year: Int,
        val dateAdded: Int,
        val size: Int,
        val genre: Int,
        val albumArtist: Int // May be -1 if not available on older devices
    )
    
    private fun createSongFromCursor(cursor: android.database.Cursor, indices: ColumnIndices): Song? {
        return try {
            val id = cursor.getLong(indices.id)
            val title = cursor.getString(indices.title)?.trim() ?: return null
            val artist = cursor.getString(indices.artist)?.trim() ?: "Unknown Artist"
            val album = cursor.getString(indices.album)?.trim() ?: "Unknown Album"
            val albumId = cursor.getLong(indices.albumId)
            val duration = cursor.getLong(indices.duration)
            val track = cursor.getInt(indices.track)
            val year = cursor.getInt(indices.year)
            val dateAdded = cursor.getLong(indices.dateAdded) * 1000L
            val size = cursor.getLong(indices.size)
            val genreId = cursor.getString(indices.genre)?.trim()
            val albumArtist = if (indices.albumArtist >= 0) {
                cursor.getString(indices.albumArtist)?.trim()?.takeIf { it.isNotBlank() }
            } else null

            // Skip files that are too small (likely invalid)
            if (size < 1024) { // Less than 1KB
                Log.d(TAG, "Skipping file too small: $title ($size bytes)")
                return null
            }

            // Skip files with empty titles
            if (title.isBlank() || title.equals("<unknown>", ignoreCase = true)) {
                Log.d(TAG, "Skipping file with invalid title: $title")
                return null
            }

            val contentUri: Uri = ContentUris.withAppendedId(
                MediaStore.Audio.Media.EXTERNAL_CONTENT_URI,
                id
            )

            val albumArtUri = ContentUris.withAppendedId(
                Uri.parse("content://media/external/audio/albumart"),
                albumId
            )

            Song(
                id = id.toString(),
                title = title,
                artist = artist,
                album = album,
                albumId = albumId.toString(),
                duration = duration,
                uri = contentUri,
                artworkUri = albumArtUri,
                trackNumber = track,
                year = year,
                dateAdded = dateAdded,
                genre = null, // Genre will be detected in background
                albumArtist = albumArtist
            )
        } catch (e: Exception) {
            Log.w(TAG, "Error creating song from cursor", e)
            null
        }
    }

    /**
     * Enhanced genre detection with multiple fallback methods
     * @param context The application context
     * @param songUri The URI of the song file
     * @param songId The song ID for MediaStore queries
     * @return The detected genre name, or null if not found
     */
    private fun getGenreForSong(context: Context, songUri: Uri, songId: Int): String? {
        // Method 1: Try MediaStore.Audio.Media.GENRE column (may contain genre ID or name)
        try {
            val genreFromMediaStoreColumn = getGenreFromMediaStoreColumn(songId)
            if (!genreFromMediaStoreColumn.isNullOrBlank()) {
                Log.d(TAG, "Found genre from MediaStore column: $genreFromMediaStoreColumn for song ID: $songId")
                return genreFromMediaStoreColumn
            }
        } catch (e: Exception) {
            Log.w(TAG, "Failed to get genre from MediaStore column", e)
        }

        // Method 2: Try MediaStore.Audio.Genres table lookup
        try {
            val genreFromGenresTable = getGenreNameFromMediaStore(context.contentResolver, songId)
            if (!genreFromGenresTable.isNullOrBlank()) {
                Log.d(TAG, "Found genre from Genres table: $genreFromGenresTable for song ID: $songId")
                return genreFromGenresTable
            }
        } catch (e: Exception) {
            Log.w(TAG, "Failed to get genre from Genres table", e)
        }

        // Method 3: Try MediaMetadataRetriever
        try {
            val genreFromRetriever = getGenreFromMediaMetadataRetriever(songUri)
            if (!genreFromRetriever.isNullOrBlank()) {
                Log.d(TAG, "Found genre from MediaMetadataRetriever: $genreFromRetriever for song URI: $songUri")
                return genreFromRetriever
            }
        } catch (e: Exception) {
            Log.w(TAG, "Failed to get genre from MediaMetadataRetriever", e)
        }

        // Method 4: Try to infer genre from file path or filename patterns
        try {
            val genreFromPath = inferGenreFromPath(songUri)
            if (!genreFromPath.isNullOrBlank()) {
                Log.d(TAG, "Inferred genre from path: $genreFromPath for song URI: $songUri")
                return genreFromPath
            }
        } catch (e: Exception) {
            Log.w(TAG, "Failed to infer genre from path", e)
        }

        Log.d(TAG, "No genre found for song ID: $songId, URI: $songUri")
        return null
    }

    /**
     * Gets genre directly from MediaStore.Audio.Media.GENRE column
     * This column may contain either a genre ID or genre name depending on Android version
     */
    private fun getGenreFromMediaStoreColumn(songId: Int): String? {
        return try {
            val projection = arrayOf(MediaStore.Audio.Media.GENRE)
            val selection = "${MediaStore.Audio.Media._ID} = ?"
            val selectionArgs = arrayOf(songId.toString())

            context.contentResolver.query(
                MediaStore.Audio.Media.EXTERNAL_CONTENT_URI,
                projection,
                selection,
                selectionArgs,
                null
            )?.use { cursor ->
                if (cursor.moveToFirst()) {
                    val genreIndex = cursor.getColumnIndex(MediaStore.Audio.Media.GENRE)
                    if (genreIndex != -1) {
                        val genreValue = cursor.getString(genreIndex)?.trim()
                        if (!genreValue.isNullOrBlank()) {
                            // Check if it's a numeric genre ID or a genre name
                            val genreId = genreValue.toLongOrNull()
                            if (genreId != null && genreId > 0) {
                                // It's a genre ID, try to convert it to name
                                return getGenreNameFromId(context.contentResolver, genreId)
                            } else {
                                // It's already a genre name
                                return genreValue
                            }
                        }
                    }
                }
            }
            null
        } catch (e: Exception) {
            Log.e(TAG, "Error getting genre from MediaStore column", e)
            null
        }
    }

    /**
     * Converts a genre ID to genre name using the Genres table
     */
    private fun getGenreNameFromId(contentResolver: android.content.ContentResolver, genreId: Long): String? {
        return try {
            val projection = arrayOf(MediaStore.Audio.Genres.NAME)
            val selection = "${MediaStore.Audio.Genres._ID} = ?"
            val selectionArgs = arrayOf(genreId.toString())

            contentResolver.query(
                MediaStore.Audio.Genres.EXTERNAL_CONTENT_URI,
                projection,
                selection,
                selectionArgs,
                null
            )?.use { cursor ->
                if (cursor.moveToFirst()) {
                    val nameIndex = cursor.getColumnIndex(MediaStore.Audio.Genres.NAME)
                    if (nameIndex != -1) {
                        return cursor.getString(nameIndex)?.trim()
                    }
                }
            }
            null
        } catch (e: Exception) {
            Log.e(TAG, "Error converting genre ID to name", e)
            null
        }
    }

    /**
     * Gets genre from MediaMetadataRetriever
     */
    private fun getGenreFromMediaMetadataRetriever(songUri: Uri): String? {
        val retriever = android.media.MediaMetadataRetriever()
        return try {
            retriever.setDataSource(context, songUri)
            val genre = retriever.extractMetadata(android.media.MediaMetadataRetriever.METADATA_KEY_GENRE)
            genre?.trim()?.takeIf { it.isNotBlank() }
        } catch (e: Exception) {
            Log.e(TAG, "Error getting genre from MediaMetadataRetriever", e)
            null
        } finally {
            try {
                retriever.release()
            } catch (e: Exception) {
                Log.e(TAG, "Error releasing MediaMetadataRetriever", e)
            }
        }
    }

    /**
     * Attempts to infer genre from file path or filename patterns
     */
    private fun inferGenreFromPath(songUri: Uri): String? {
        return try {
            val path = songUri.toString().lowercase()

            // Common genre patterns in file paths
            when {
                path.contains("rock") -> "Rock"
                path.contains("pop") -> "Pop"
                path.contains("hip.hop") || path.contains("hiphop") || path.contains("rap") -> "Hip Hop"
                path.contains("jazz") -> "Jazz"
                path.contains("classical") || path.contains("classic") -> "Classical"
                path.contains("electronic") || path.contains("electro") || path.contains("edm") -> "Electronic"
                path.contains("country") -> "Country"
                path.contains("blues") -> "Blues"
                path.contains("reggae") -> "Reggae"
                path.contains("folk") -> "Folk"
                path.contains("metal") -> "Metal"
                path.contains("punk") -> "Punk"
                path.contains("indie") -> "Indie"
                path.contains("alternative") -> "Alternative"
                path.contains("r&b") || path.contains("rnb") -> "R&B"
                path.contains("soul") -> "Soul"
                path.contains("funk") -> "Funk"
                path.contains("disco") -> "Disco"
                path.contains("dance") -> "Dance"
                path.contains("house") -> "House"
                path.contains("techno") -> "Techno"
                path.contains("trance") -> "Trance"
                path.contains("ambient") -> "Ambient"
                path.contains("soundtrack") || path.contains("ost") -> "Soundtrack"
                path.contains("instrumental") -> "Instrumental"
                path.contains("vocal") -> "Vocal"
                path.contains("christmas") || path.contains("holiday") -> "Holiday"
                path.contains("world") -> "World"
                path.contains("latin") -> "Latin"
                path.contains("african") -> "African"
                path.contains("asian") -> "Asian"
                else -> null
            }
        } catch (e: Exception) {
            Log.e(TAG, "Error inferring genre from path", e)
            null
        }
    }

    /**
     * Gets the genre name from MediaStore.Audio.Genres table using the song ID
     * @param contentResolver The ContentResolver to use for queries
     * @param songId The song ID to look up genre for
     * @return The genre name, or null if not found
     */
    private fun getGenreNameFromMediaStore(contentResolver: android.content.ContentResolver, songId: Int): String? {
        return try {
            // Try to get genre directly from the URI - works on newer Android versions
            val genreUri = android.provider.MediaStore.Audio.Genres.getContentUriForAudioId("external", songId)
            val projection = arrayOf(android.provider.MediaStore.Audio.Genres.NAME)
            
            contentResolver.query(
                genreUri,
                projection,
                null,
                null,
                null
            )?.use { cursor ->
                if (cursor.moveToFirst()) {
                    val nameIndex = cursor.getColumnIndexOrThrow(android.provider.MediaStore.Audio.Genres.NAME)
                    val genreName = cursor.getString(nameIndex)
                    if (!genreName.isNullOrBlank()) {
                        Log.d(TAG, "Found genre: $genreName for song ID: $songId")
                        return genreName
                    }
                }
            }

            null
        } catch (e: IllegalArgumentException) {
            // Column doesn't exist on this Android version - silently ignore
            Log.d(TAG, "Genre column not available on this device (Android API limitation)")
            null
        } catch (e: Exception) {
            // Other errors - log but don't spam
            if (Log.isLoggable(TAG, Log.DEBUG)) {
                Log.d(TAG, "Could not get genre for song ID: $songId: ${e.message}")
            }
            null
        }
    }

    suspend fun loadAlbums(): List<Album> = withContext(Dispatchers.IO) {
        val albums = mutableListOf<Album>()
        val collection = MediaStore.Audio.Albums.EXTERNAL_CONTENT_URI

        val projection = arrayOf(
            MediaStore.Audio.Albums._ID,
            MediaStore.Audio.Albums.ALBUM,
            MediaStore.Audio.Albums.ARTIST,
            MediaStore.Audio.Albums.NUMBER_OF_SONGS,
            MediaStore.Audio.Albums.FIRST_YEAR
        )

        val sortOrder = "${MediaStore.Audio.Albums.ALBUM} ASC"

        // Load all songs once
        val allSongs = loadSongs()
        val songsByAlbumTitle = allSongs.groupBy { it.album }

        context.contentResolver.query(
            collection,
            projection,
            null,
            null,
            sortOrder
        )?.use { cursor ->
            // Cache column indices
            val idColumn = cursor.getColumnIndexOrThrow(MediaStore.Audio.Albums._ID)
            val albumColumn = cursor.getColumnIndexOrThrow(MediaStore.Audio.Albums.ALBUM)
            val artistColumn = cursor.getColumnIndexOrThrow(MediaStore.Audio.Albums.ARTIST)
            val songsCountColumn =
                cursor.getColumnIndexOrThrow(MediaStore.Audio.Albums.NUMBER_OF_SONGS)
            val yearColumn = cursor.getColumnIndexOrThrow(MediaStore.Audio.Albums.FIRST_YEAR)

            while (cursor.moveToNext()) {
                val id = cursor.getLong(idColumn)
                val title = cursor.getString(albumColumn)
                val artist = cursor.getString(artistColumn)
                val songsCount = cursor.getInt(songsCountColumn)
                val year = cursor.getInt(yearColumn)

                val albumArtUri = ContentUris.withAppendedId(
                    Uri.parse("content://media/external/audio/albumart"),
                    id
                )

                // Get songs for this album from the pre-loaded map
                val albumSongs = songsByAlbumTitle[title] ?: emptyList()

                val album = Album(
                    id = id.toString(),
                    title = title,
                    artist = artist,
                    artworkUri = albumArtUri,
                    year = year,
                    songs = albumSongs,
                    numberOfSongs = albumSongs.size
                )
                albums.add(album)
            }
        }

        Log.d(TAG, "Loaded ${albums.size} albums")
        albums
    }

    /**
     * Loads artists from device storage with enhanced metadata extraction.
     * Supports grouping by album artist or track artist based on user preference.
     */
    suspend fun loadArtists(): List<Artist> = withContext(Dispatchers.IO) {
        val appSettings = AppSettings.getInstance(context)
        val groupByAlbumArtist = appSettings.groupByAlbumArtist.value
        
        Log.d(TAG, "Loading artists (groupByAlbumArtist=$groupByAlbumArtist)")
        
        if (groupByAlbumArtist) {
            // New method: Group by album artist from songs
            loadArtistsGroupedByAlbumArtist()
        } else {
            // Original method: Use MediaStore.Audio.Artists (grouped by track artist)
            loadArtistsFromMediaStore()
        }
    }
    
    /**
     * Original method: Loads artists using MediaStore.Audio.Artists
     * This groups by track artist, showing collaborations as separate entries
     */
    private suspend fun loadArtistsFromMediaStore(): List<Artist> = withContext(Dispatchers.IO) {
        val artists = mutableListOf<Artist>()
        val collection = MediaStore.Audio.Artists.EXTERNAL_CONTENT_URI

        val projection = arrayOf(
            MediaStore.Audio.Artists._ID,
            MediaStore.Audio.Artists.ARTIST,
            MediaStore.Audio.Artists.NUMBER_OF_ALBUMS,
            MediaStore.Audio.Artists.NUMBER_OF_TRACKS
        )

        val selection = "${MediaStore.Audio.Artists.ARTIST} != ''"
        val sortOrder = "${MediaStore.Audio.Artists.ARTIST} ASC"

        context.contentResolver.query(
            collection,
            projection,
            selection,
            null,
            sortOrder
        )?.use { cursor ->
            // Cache column indices
            val idColumn = cursor.getColumnIndexOrThrow(MediaStore.Audio.Artists._ID)
            val artistColumn = cursor.getColumnIndexOrThrow(MediaStore.Audio.Artists.ARTIST)
            val albumsColumn =
                cursor.getColumnIndexOrThrow(MediaStore.Audio.Artists.NUMBER_OF_ALBUMS)
            val tracksColumn =
                cursor.getColumnIndexOrThrow(MediaStore.Audio.Artists.NUMBER_OF_TRACKS)

            while (cursor.moveToNext()) {
                val id = cursor.getLong(idColumn)
                var name = cursor.getString(artistColumn)
                val numAlbums = cursor.getInt(albumsColumn)
                val numTracks = cursor.getInt(tracksColumn)

                // Clean up artist name
                if (name.isNullOrBlank() || name.equals("<unknown>", ignoreCase = true)) {
                    // Try to find a better name from the artist's tracks
                    name = findBetterArtistName(id.toString()) ?: "Unknown Artist"
                }

                val artist = Artist(
                    id = id.toString(),
                    name = name,
                    numberOfAlbums = numAlbums,
                    numberOfTracks = numTracks
                )
                artists.add(artist)
            }
        }

        Log.d(TAG, "Loaded ${artists.size} artists from MediaStore")
        artists
    }
    
    /**
     * New method: Groups artists by album artist from all songs.
     * This provides proper album-based grouping, showing single artist for collaboration albums.
     */
    private suspend fun loadArtistsGroupedByAlbumArtist(): List<Artist> = withContext(Dispatchers.IO) {
        val allSongs = loadSongs()
        val artistMap = mutableMapOf<String, MutableList<Song>>()
        val albumsByArtist = mutableMapOf<String, MutableSet<String>>()
        
        // Group songs by album artist (or track artist if album artist is not available)
        for (song in allSongs) {
            val artistName = (song.albumArtist?.takeIf { it.isNotBlank() } ?: song.artist).trim()
            
            // Skip invalid artist names
            if (artistName.isBlank() || artistName.equals("<unknown>", ignoreCase = true)) {
                continue
            }
            
            // Add song to artist's collection
            artistMap.getOrPut(artistName) { mutableListOf() }.add(song)
            
            // Track unique albums for this artist
            if (song.album.isNotBlank()) {
                albumsByArtist.getOrPut(artistName) { mutableSetOf() }.add(song.album)
            }
        }
        
        // Create Artist objects from grouped data
        val artists = artistMap.map { (artistName, songs) ->
            val albums = albumsByArtist[artistName] ?: emptySet()
            Artist(
                id = "album_artist_${artistName.hashCode()}", // Generate unique ID based on name
                name = artistName,
                numberOfAlbums = albums.size,
                numberOfTracks = songs.size
            )
        }.sortedBy { it.name.lowercase() }
        
        Log.d(TAG, "Loaded ${artists.size} artists grouped by album artist")
        artists
    }

    /**
     * Tries to find a better artist name from their tracks
     */
    private suspend fun findBetterArtistName(artistId: String): String? =
        withContext(Dispatchers.IO) {
            val uri = MediaStore.Audio.Media.EXTERNAL_CONTENT_URI
            val selection = "${MediaStore.Audio.Media.ARTIST_ID} = ?"
            val selectionArgs = arrayOf(artistId)

            context.contentResolver.query(
                uri,
                arrayOf(MediaStore.Audio.Media.ARTIST),
                selection,
                selectionArgs,
                null
            )?.use { cursor ->
                val artistColumn = cursor.getColumnIndexOrThrow(MediaStore.Audio.Media.ARTIST)
                val artists = mutableSetOf<String>()

                while (cursor.moveToNext()) {
                    val artist = cursor.getString(artistColumn)
                    if (!artist.isNullOrBlank() && !artist.equals("<unknown>", ignoreCase = true)) {
                        artists.add(artist)
                    }
                }

                // Return the most common non-blank artist name
                artists.maxByOrNull { name ->
                    cursor.moveToFirst()
                    var count = 0
                    while (cursor.moveToNext()) {
                        if (cursor.getString(artistColumn) == name) count++
                    }
                    count
                }
            }
        }

    /**
     * Triggers a full rescan of music data from the device's MediaStore.
     * This method will reload songs, albums, and artists.
     */
    suspend fun refreshMusicData() {
        Log.d(TAG, "Refreshing music data...")
        // Simply calling loadSongs, loadAlbums, loadArtists will re-query MediaStore
        // and provide fresh data. The ViewModel will then update its StateFlows.
        // No need to return anything here, as the ViewModel will call these methods
        // and collect the results.
        loadSongs()
        loadAlbums()
        loadArtists()
        Log.d(TAG, "Music data refresh complete.")
    }

    /**
     * Fetches artist images from Deezer API for artists without images
     */
    suspend fun fetchArtistImages(artists: List<Artist>): List<Artist> =
        withContext(Dispatchers.IO) {
            val updatedArtists = mutableListOf<Artist>()

            // NetworkClient will handle API key dynamically (user-provided or fallback to default)

            for (artist in artists) {
                try {
                    Log.d(TAG, "Processing artist: ${artist.name}")

                    if (artist.artworkUri != null) {
                        Log.d(
                            TAG,
                            "Artist ${artist.name} already has artwork: ${artist.artworkUri}"
                        )
                        updatedArtists.add(artist)
                        continue
                    }

                    // Check cache first
                    val cachedUri = artistImageCache[artist.name]
                    if (cachedUri != null) {
                        Log.d(TAG, "Using cached image for artist: ${artist.name}")
                        updatedArtists.add(artist.copy(artworkUri = cachedUri))
                        continue
                    }

                    // Check local storage for artist image
                    val localImage = findLocalArtistImage(artist.name)
                    if (localImage != null) {
                        Log.d(TAG, "Using local image for artist: ${artist.name}")
                        artistImageCache[artist.name] = localImage
                        updatedArtists.add(artist.copy(artworkUri = localImage))
                        continue
                    }

                    // Skip artists with empty or "Unknown" names
                    if (artist.name.isBlank() || artist.name.equals(
                            "Unknown Artist",
                            ignoreCase = true
                        )
                    ) {
                        Log.d(TAG, "Skipping unknown/blank artist name")
                        val placeholderUri =
                            chromahub.rhythm.app.util.ImageUtils.generatePlaceholderImage(
                                name = "Unknown Artist",
                                size = 500,
                                cacheDir = context.cacheDir
                            )
                        artistImageCache[artist.name] = placeholderUri
                        updatedArtists.add(artist.copy(artworkUri = placeholderUri))
                        continue
                    }

                    // Only try online fetch if network is available and Deezer API is enabled
                    if (isNetworkAvailable() && NetworkClient.isDeezerApiEnabled()) {
                        Log.d(TAG, "Searching for artist on Deezer: ${artist.name}")
                        
                        // Intelligent delay based on API and previous request timing
                        val apiDelay = calculateApiDelay("deezer", System.currentTimeMillis())
                        if (apiDelay > 0) {
                            delay(apiDelay)
                        }
                        updateLastApiCall("deezer", System.currentTimeMillis())
                        
                        try {
                            var deezerArtist: DeezerArtist? = null
                            
                            // First attempt: exact artist name with fuzzy matching
                            var searchResponse = deezerApiService.searchArtists(artist.name)
                            deezerArtist = findBestMatch(searchResponse.data, artist.name)
                            
                            // Second attempt: try with cleaned artist name if first failed
                            if (deezerArtist == null && artist.name.isNotBlank()) {
                                val cleanedName = artist.name
                                    .replace(Regex("\\s*\\([^)]*\\)\\s*"), "") // Remove text in parentheses
                                    .replace(Regex("\\s*&\\s*.*"), "") // Remove everything after &
                                    .replace(Regex("\\s*,\\s*.*"), "") // Remove everything after comma
                                    .replace(Regex("\\s*feat\\.?\\s*.*", RegexOption.IGNORE_CASE), "") // Remove feat.
                                    .replace(Regex("\\s*ft\\.?\\s*.*", RegexOption.IGNORE_CASE), "") // Remove ft.
                                    .trim()
                                
                                if (cleanedName.isNotEmpty() && cleanedName != artist.name) {
                                    Log.d(TAG, "Retrying Deezer search with cleaned name: $cleanedName")
                                    searchResponse = deezerApiService.searchArtists(cleanedName)
                                    deezerArtist = findBestMatch(searchResponse.data, artist.name)
                                }
                            }
                            
                            // Third attempt: try with first word only for multi-word artists
                            if (deezerArtist == null && artist.name.contains(" ")) {
                                val firstWord = artist.name.split(" ").first().trim()
                                if (firstWord.isNotEmpty() && firstWord.length > 2) {
                                    Log.d(TAG, "Retrying Deezer search with first word: $firstWord")
                                    searchResponse = deezerApiService.searchArtists(firstWord)
                                    deezerArtist = findBestMatch(searchResponse.data, artist.name)
                                }
                            }

                            if (deezerArtist != null) {
                                Log.d(TAG, "Found Deezer artist: ${deezerArtist.name} for ${artist.name}")
                                
                                // Choose the highest quality image available
                                val imageUrl = when {
                                    !deezerArtist.pictureXl.isNullOrEmpty() -> deezerArtist.pictureXl
                                    !deezerArtist.pictureBig.isNullOrEmpty() -> deezerArtist.pictureBig
                                    !deezerArtist.pictureMedium.isNullOrEmpty() -> deezerArtist.pictureMedium
                                    !deezerArtist.picture.isNullOrEmpty() -> deezerArtist.picture
                                    else -> null
                                }
                                
                                if (!imageUrl.isNullOrEmpty()) {
                                    val imageUri = Uri.parse(imageUrl)
                                    Log.d(TAG, "Found image URL for ${artist.name}: $imageUrl")
                                    artistImageCache[artist.name] = imageUri
                                    saveLocalArtistImage(artist.name, imageUrl)
                                    updatedArtists.add(artist.copy(artworkUri = imageUri))
                                    continue
                                }
                            } else {
                                Log.d(TAG, "No Deezer artist found for: ${artist.name}")
                            }
                        } catch (e: Exception) {
                            Log.w(TAG, "Deezer lookup failed for ${artist.name}: ${e.message}")
                        }
                    } else {
                        Log.d(
                            TAG,
                            "No network connection available while fetching ${artist.name} image"
                        )
                    }

                    // -------- YouTube Music fallback (right after Deezer fails) --------
                    if (NetworkClient.isYTMusicApiEnabled()) {
                        try {
                            Log.d(TAG, "Trying YTMusic for artist: ${artist.name}")
                            
                        // Create search request for artist
                        val searchRequest = YTMusicSearchRequest(
                            context = YTMusicContext(YTMusicClient()),
                            query = artist.name,
                            params = "EgWKAQIIAWoKEAoQAxAEEAkQBQ%3D%3D" // Artist search filter
                        )
                        
                        val searchResponse = ytmusicApiService.search(request = searchRequest)
                        if (searchResponse.isSuccessful) {
                            val imageUrl = searchResponse.body()?.extractArtistImageUrl()
                            if (!imageUrl.isNullOrEmpty()) {
                                val imageUri = Uri.parse(imageUrl)
                                artistImageCache[artist.name] = imageUri
                                saveLocalArtistImage(artist.name, imageUrl)
                                updatedArtists.add(artist.copy(artworkUri = imageUri))
                                Log.d(TAG, "Found YTMusic image for ${artist.name}: $imageUrl")
                                continue
                            }
                            
                            // If search result has browseId, try to get detailed artist info for better quality image
                            val browseId = searchResponse.body()?.extractArtistBrowseId()
                            if (!browseId.isNullOrEmpty()) {
                                val browseRequest = YTMusicBrowseRequest(
                                    context = YTMusicContext(YTMusicClient()),
                                    browseId = browseId
                                )
                                val artistResponse = ytmusicApiService.getArtist(request = browseRequest)
                                if (artistResponse.isSuccessful) {
                                    val detailedImageUrl = artistResponse.body()?.extractArtistThumbnail()
                                    if (!detailedImageUrl.isNullOrEmpty()) {
                                        val imageUri = Uri.parse(detailedImageUrl)
                                        artistImageCache[artist.name] = imageUri
                                        saveLocalArtistImage(artist.name, detailedImageUrl)
                                        updatedArtists.add(artist.copy(artworkUri = imageUri))
                                        Log.d(TAG, "Found detailed YTMusic image for ${artist.name}: $detailedImageUrl")
                                        continue
                                    }
                                }
                            }
                        }
                        } catch (e: Exception) {
                            Log.w(TAG, "YTMusic fallback failed for ${artist.name}: ${e.message}")
                        }
                    }

                    // If we get here, generate a placeholder image
                    Log.d(TAG, "Generating placeholder for artist: ${artist.name}")
                    val placeholderUri =
                        chromahub.rhythm.app.util.ImageUtils.generatePlaceholderImage(
                            name = artist.name,
                            size = 500,
                            cacheDir = context.cacheDir
                        )
                    artistImageCache[artist.name] = placeholderUri
                    updatedArtists.add(artist.copy(artworkUri = placeholderUri))

                } catch (e: Exception) {
                    Log.e(TAG, "Error fetching artist image for ${artist.name}", e)
                    try {
                        val placeholderUri =
                            chromahub.rhythm.app.util.ImageUtils.generatePlaceholderImage(
                                name = artist.name,
                                size = 500,
                                cacheDir = context.cacheDir
                            )
                        updatedArtists.add(artist.copy(artworkUri = placeholderUri))
                    } catch (e2: Exception) {
                        Log.e(TAG, "Error generating placeholder for ${artist.name}", e2)
                        updatedArtists.add(artist)
                    }
                }
            }

            updatedArtists
        }
    /**
     * Extracts embedded lyrics from audio file metadata
     * 
     * This method attempts multiple strategies to extract lyrics:
     * 1. MediaMetadataRetriever with common metadata keys
     * 2. Direct file reading for ID3v2 USLT (Unsynchronized Lyrics) frames
     * 
     * Note: Android's MediaMetadataRetriever has limited lyrics support.
     * ID3v2.3/v2.4 tags with USLT frames are the most reliable format.
     */
    private fun getEmbeddedLyrics(songUri: Uri): LyricsData? {
        // Try MediaMetadataRetriever first (fastest and most standard way)
        val retrieverLyrics = extractLyricsViaRetriever(songUri)
        if (retrieverLyrics != null) {
            Log.d(TAG, "Successfully extracted lyrics via MediaMetadataRetriever.")
            return retrieverLyrics
        }
        
        // As a fallback, try direct file reading for ID3 tags
        val id3Lyrics = extractLyricsFromID3(songUri)
        if (id3Lyrics != null) {
            Log.d(TAG, "Successfully extracted lyrics via direct ID3 tag parsing.")
            return id3Lyrics
        }
        
        Log.d(TAG, "No embedded lyrics found for URI: $songUri")
        return null
    }
    
    /**
     * Attempts to extract lyrics using MediaMetadataRetriever. This is the preferred
     * method as it's handled by the Android system.
     */
    private fun extractLyricsViaRetriever(songUri: Uri): LyricsData? {
        var retriever: android.media.MediaMetadataRetriever? = null
        try {
            retriever = android.media.MediaMetadataRetriever()
            retriever.setDataSource(context, songUri)
            
            // Try common metadata keys that might contain lyrics
            // Note: Standard key codes documented in MediaMetadataRetriever
            val potentialLyricsKeys = listOf(
                // Some devices/implementations may use these keys for lyrics
                android.media.MediaMetadataRetriever.METADATA_KEY_WRITER, 
                android.media.MediaMetadataRetriever.METADATA_KEY_COMPOSER,
                // Try some numeric codes (vendor-specific, may work on some devices)
                1000, 1001, 1002, 1003
            )
            
            for (key in potentialLyricsKeys) {
                try {
                    val metadata = retriever.extractMetadata(key)
                    if (!metadata.isNullOrBlank() && looksLikeLyrics(metadata)) {
                        Log.d(TAG, "Found embedded lyrics via retriever key: $key")
                        return parseLyricsData(metadata)
                    }
                } catch (e: Exception) {
                    // Key not supported, continue
                    continue
                }
            }
        } catch (e: Exception) {
            Log.w(TAG, "MediaMetadataRetriever extraction failed: ${e.message}")
        } finally {
            try {
                retriever?.release()
            } catch (e: Exception) {
                Log.w(TAG, "Error releasing MediaMetadataRetriever: ${e.message}")
            }
        }
        return null
    }
    
    /**
     * Attempts to extract lyrics by reading ID3v2 tags directly from file
     * Specifically looks for USLT (Unsynchronized Lyrics) frames
     */
    private fun extractLyricsFromID3(songUri: Uri): LyricsData? {
        try {
            // Get file path from URI
            val filePath = getFilePathFromUri(songUri) ?: return null
            val file = java.io.File(filePath)
            
            if (!file.exists() || !file.canRead()) {
                Log.d(TAG, "Cannot read file for ID3 extraction: $filePath")
                return null
            }
            
            // Read ID3v2 tag
            java.io.RandomAccessFile(file, "r").use { raf ->
                // Check for ID3v2 header
                val header = ByteArray(10)
                raf.read(header)
                
                if (header[0] == 'I'.code.toByte() && 
                    header[1] == 'D'.code.toByte() && 
                    header[2] == '3'.code.toByte()) {
                    
                    // ID3v2 tag found
                    val version = header[3].toInt()
                    Log.d(TAG, "Found ID3v2.$version tag")
                    
                    // Calculate tag size (synchsafe integer)
                    val tagSize = ((header[6].toInt() and 0x7F) shl 21) or
                                 ((header[7].toInt() and 0x7F) shl 14) or
                                 ((header[8].toInt() and 0x7F) shl 7) or
                                 (header[9].toInt() and 0x7F)
                    
                    // Read tag data
                    val tagData = ByteArray(tagSize)
                    raf.read(tagData)
                    
                    // Search for USLT frame
                    val lyrics = findUSLTFrame(tagData)
                    if (lyrics != null) {
                        Log.d(TAG, "Found USLT lyrics in ID3 tag (length: ${lyrics.length})")
                        return parseLyricsData(lyrics)
                    }
                }
            }
        } catch (e: Exception) {
            Log.w(TAG, "ID3 tag extraction failed: ${e.message}")
        }
        return null
    }
    
    /**
     * Searches for USLT (Unsynchronized Lyrics) frame in ID3v2 tag data
     */
    private fun findUSLTFrame(tagData: ByteArray): String? {
        var pos = 0
        while (pos < tagData.size - 10) {
            try {
                // Check for USLT frame header
                if (tagData[pos] == 'U'.code.toByte() &&
                    tagData[pos + 1] == 'S'.code.toByte() &&
                    tagData[pos + 2] == 'L'.code.toByte() &&
                    tagData[pos + 3] == 'T'.code.toByte()) {
                    
                    // Frame size (4 bytes after frame ID)
                    val frameSize = ((tagData[pos + 4].toInt() and 0xFF) shl 24) or
                                   ((tagData[pos + 5].toInt() and 0xFF) shl 16) or
                                   ((tagData[pos + 6].toInt() and 0xFF) shl 8) or
                                   (tagData[pos + 7].toInt() and 0xFF)
                    
                    if (frameSize > 0 && frameSize < tagData.size - pos) {
                        // Skip frame header (10 bytes) + encoding (1 byte) + language (3 bytes) + descriptor
                        var dataPos = pos + 14
                        
                        // Skip descriptor (null-terminated string)
                        while (dataPos < pos + frameSize && tagData[dataPos] != 0.toByte()) {
                            dataPos++
                        }
                        dataPos++ // Skip null terminator
                        
                        // Extract lyrics text
                        val lyricsLength = (pos + 10 + frameSize) - dataPos
                        if (lyricsLength > 0) {
                            val lyricsBytes = tagData.copyOfRange(dataPos, dataPos + lyricsLength)
                            val lyrics = String(lyricsBytes, Charsets.UTF_8).trim()
                            if (lyrics.isNotBlank()) {
                                return lyrics
                            }
                        }
                    }
                }
                pos++
            } catch (e: Exception) {
                // Continue searching
                pos++
            }
        }
        return null
    }
    
    /**
     * Helper to determine if text looks like lyrics
     */
    private fun looksLikeLyrics(text: String): Boolean {
        val hasMultipleLines = text.contains("\n")
        val hasTimestamp = text.contains(Regex("\\[\\d{2}:\\d{2}"))
        val isLongEnough = text.length > 50
        val hasCommonLyricsWords = text.lowercase().let { 
            it.contains("verse") || it.contains("chorus") || it.contains("bridge")
        }
        
        return hasMultipleLines || hasTimestamp || (isLongEnough && !hasCommonLyricsWords)
    }
    
    /**
     * Parses lyrics text into LyricsData with proper format detection
     */
    private fun parseLyricsData(lyrics: String): LyricsData {
        // Check if lyrics are synced (contain LRC-style timestamps)
        val isSynced = lyrics.contains(Regex("\\[\\d{2}:\\d{2}\\.\\d{2}]"))
        
        return if (isSynced) {
            LyricsData(null, lyrics, null)
        } else {
            LyricsData(lyrics, null, null)
        }
    }
    
    /**
     * Helper to get file path from content URI
     */
    private fun getFilePathFromUri(uri: Uri): String? {
        if (uri.scheme == "file") {
            return uri.path
        }
        
        if (uri.scheme == "content") {
            try {
                context.contentResolver.query(uri, arrayOf(android.provider.MediaStore.Audio.Media.DATA), null, null, null)?.use { cursor ->
                    if (cursor.moveToFirst()) {
                        val columnIndex = cursor.getColumnIndex(android.provider.MediaStore.Audio.Media.DATA)
                        if (columnIndex >= 0) {
                            return cursor.getString(columnIndex)
                        }
                    }
                }
            } catch (e: Exception) {
                Log.w(TAG, "Failed to get file path from URI: ${e.message}")
            }
        }
        
        return null
    }

    /**
     * Fetches lyrics for a song using various sources based on user preference.
     * @param artist Song artist
     * @param title Song title
     * @param songId Optional song ID for cache key - prevents wrong lyrics for songs with similar names
     * @param songUri Optional song URI for embedded lyrics extraction
     * @param sourcePreference User's preferred lyrics source order
     */
    suspend fun fetchLyrics(
        artist: String,
        title: String,
        songId: String? = null,
        songUri: Uri? = null,
        sourcePreference: LyricsSourcePreference = LyricsSourcePreference.API_FIRST
    ): LyricsData? = withContext(Dispatchers.IO) {
        if (artist.isBlank() || title.isBlank()) {
            Log.w(TAG, "Cannot fetch lyrics for blank artist or title.")
            return@withContext LyricsData("No lyrics available for this song", null, null)
        }

        val cacheKey = if (songId != null) {
            "$songId:$artist:$title".lowercase()
        } else {
            "$artist:$title".lowercase()
        }

        lyricsCache[cacheKey]?.let {
            Log.d(TAG, "Returning cached lyrics for: $artist - $title")
            return@withContext it
        }

        Log.d(TAG, "Fetching lyrics for '$title' by '$artist'. Source preference: $sourcePreference")
        if (songUri == null) {
            Log.w(TAG, "Song URI is null, cannot fetch embedded lyrics.")
        }

        val sourcesInOrder = when (sourcePreference) {
            LyricsSourcePreference.API_FIRST -> listOf("API", "Embedded", "Local")
            LyricsSourcePreference.EMBEDDED_FIRST -> listOf("Embedded", "API", "Local")
            LyricsSourcePreference.LOCAL_FIRST -> listOf("Local", "Embedded", "API")
        }

        for (source in sourcesInOrder) {
            Log.d(TAG, "Trying source: $source")
            val lyrics: LyricsData? = try {
                when (source) {
                    "API" -> {
                        if (isNetworkAvailable()) {
                            fetchLyricsFromAPIs(artist, title)
                        } else {
                            Log.d(TAG, "Network not available, skipping API fetch.")
                            null
                        }
                    }
                    "Embedded" -> {
                        songUri?.let { getEmbeddedLyrics(it) }
                    }
                    "Local" -> {
                        findLocalLyrics(artist, title)
                    }
                    else -> null
                }
            } catch (e: Exception) {
                Log.w(TAG, "Error fetching lyrics from source '$source'", e)
                null
            }

            if (lyrics != null && lyrics.hasLyrics()) {
                Log.d(TAG, "Success! Found lyrics from $source for: $artist - $title")
                lyricsCache[cacheKey] = lyrics
                if (source == "API") {
                    saveLocalLyrics(artist, title, lyrics)
                }
                return@withContext lyrics
            } else {
                Log.d(TAG, "No lyrics found from source: $source")
            }
        }

        // No lyrics found from any source
        Log.d(TAG, "No lyrics found from any source for: $artist - $title")
        val notFoundLyrics = LyricsData("No lyrics found for this song", null, null)
        lyricsCache[cacheKey] = notFoundLyrics
        return@withContext notFoundLyrics
    }
    
    /**
     * Fetches lyrics from online APIs (Apple Music, LRCLib, etc.)
     * Extracted as a separate method for cleaner code
     */
    private suspend fun fetchLyricsFromAPIs(artist: String, title: String): LyricsData? {
        val cleanArtist = artist.trim().replace(Regex("\\(.*?\\)"), "").trim()
        val cleanTitle = title.trim().replace(Regex("\\(.*?\\)"), "").trim()

        var plainLyrics: String? = null
        var syncedLyrics: String? = null
        var wordByWordLyrics: String? = null

            // ---- Apple Music (Word-by-word synchronized lyrics - highest priority) ----
            if (NetworkClient.isAppleMusicApiEnabled()) {
                try {
                    Log.d(TAG, "Attempting Apple Music lyrics search for: $cleanTitle by $cleanArtist")
                    val query = "$cleanTitle $cleanArtist"
                    val searchResults = appleMusicApiService.searchSongs(query)
                    
                    if (searchResults.isNotEmpty()) {
                        // Find best match
                        val bestMatch = searchResults.firstOrNull { result ->
                            val artistMatch = result.artistName?.lowercase()?.contains(cleanArtist.lowercase()) == true ||
                                cleanArtist.lowercase().contains(result.artistName?.lowercase() ?: "")
                            val titleMatch = result.songName?.lowercase()?.contains(cleanTitle.lowercase()) == true ||
                                cleanTitle.lowercase().contains(result.songName?.lowercase() ?: "")
                            artistMatch && titleMatch
                        } ?: searchResults.firstOrNull() // Fallback to first result if no exact match
                        
                        bestMatch?.let { match ->
                            Log.d(TAG, "Found Apple Music match: ${match.songName} by ${match.artistName} (ID: ${match.id})")
                            try {
                                val lyricsResponse = appleMusicApiService.getLyrics(match.id)
                                
                                // Check if track has time-synced lyrics flag
                                val hasTimeSyncedLyrics = lyricsResponse.track?.hasTimeSyncedLyrics == true
                                Log.d(TAG, "Apple Music track hasTimeSyncedLyrics: $hasTimeSyncedLyrics, type: ${lyricsResponse.type}")
                                
                                // Check if we have word-by-word lyrics (Syllable type) or line-synced lyrics
                                if (!lyricsResponse.content.isNullOrEmpty() && hasTimeSyncedLyrics) {
                                    
                                    // Extract plain text from lyrics content
                                    val plainText = lyricsResponse.content.mapNotNull { line ->
                                        line.text?.joinToString(" ") { word -> word.text }
                                    }.joinToString("\n")
                                    
                                    if (plainText.isNotEmpty()) {
                                        plainLyrics = plainText
                                    }
                                    
                                    // Check for word-by-word (Syllable) lyrics
                                    if (lyricsResponse.type == "Syllable") {
                                        // Convert to JSON string to store in LyricsData
                                        wordByWordLyrics = com.google.gson.Gson().toJson(lyricsResponse.content)
                                        Log.d(TAG, "Apple Music word-by-word lyrics found (${lyricsResponse.content.size} lines)")
                                    } else {
                                        // Fall back to line-synced lyrics format
                                        val syncedLyricsText = lyricsResponse.content.mapNotNull { line ->
                                            val timestamp = line.timestamp ?: return@mapNotNull null
                                            val text = line.text?.joinToString(" ") { word -> word.text } ?: return@mapNotNull null
                                            val minutes = timestamp / 60000
                                            val seconds = (timestamp % 60000) / 1000
                                            val millis = (timestamp % 1000) / 10
                                            String.format("[%02d:%02d.%02d]%s", minutes, seconds, millis, text)
                                        }.joinToString("\n")
                                        
                                        if (syncedLyricsText.isNotEmpty()) {
                                            syncedLyrics = syncedLyricsText
                                            Log.d(TAG, "Apple Music line-synced lyrics found (${lyricsResponse.content.size} lines)")
                                        }
                                    }
                                    
                                    val lyricsData = LyricsData(plainLyrics, syncedLyrics, wordByWordLyrics)
                                    return lyricsData
                                } else {
                                    Log.d(TAG, "Apple Music lyrics not time-synced or empty for: ${match.songName}")
                                }
                            } catch (e: Exception) {
                                Log.e(TAG, "Error fetching Apple Music lyrics for ID ${match.id}: ${e.message}", e)
                            }
                        }
                    } else {
                        Log.d(TAG, "No Apple Music results found for: $query")
                    }
                } catch (e: Exception) {
                    Log.e(TAG, "Apple Music lyrics search failed: ${e.message}", e)
                }
            }

            // ---- LRCLib (Enhanced search with multiple strategies - line-by-line synced) ----
            if (NetworkClient.isLrcLibApiEnabled()) {
                try {
                    // Strategy 1: Search by track name and artist name
                    var results =
                    lrclibApiService.searchLyrics(trackName = cleanTitle, artistName = cleanArtist)

                // If no results, try fallback strategies
                if (results.isEmpty()) {
                    // Strategy 2: Search with generic query combining artist and title
                    val query = "$cleanArtist $cleanTitle"
                    results = lrclibApiService.searchLyrics(query = query)
                }

                if (results.isEmpty()) {
                    // Strategy 3: Try without parenthetical content and with simplified names
                    val simplifiedArtist =
                        cleanArtist.split(" feat.", " ft.", " featuring").first().trim()
                    val simplifiedTitle =
                        cleanTitle.split(" feat.", " ft.", " featuring").first().trim()
                    results = lrclibApiService.searchLyrics(
                        trackName = simplifiedTitle,
                        artistName = simplifiedArtist
                    )
                }

                // Find the best match - prioritize exact matches, then synced lyrics, then any lyrics
                val bestMatch = results.firstOrNull { result ->
                    val artistMatch =
                        result.artistName?.lowercase()?.contains(cleanArtist.lowercase()) == true ||
                                cleanArtist.lowercase()
                                    .contains(result.artistName?.lowercase() ?: "")
                    val titleMatch =
                        result.trackName?.lowercase()?.contains(cleanTitle.lowercase()) == true ||
                                cleanTitle.lowercase().contains(result.trackName?.lowercase() ?: "")

                    (artistMatch && titleMatch) && result.hasLyrics()
                } ?: results.firstOrNull { it.hasSyncedLyrics() } // Prefer synced lyrics
                ?: results.firstOrNull { it.hasLyrics() } // Then any lyrics

                bestMatch?.let { bm ->
                    syncedLyrics = bm.getSyncedLyricsOrNull()
                    plainLyrics = bm.getPlainLyricsOrNull()

                    if (syncedLyrics != null || plainLyrics != null) {
                        Log.d(
                            TAG,
                            "LRCLib lyrics found - Synced: ${syncedLyrics != null}, Plain: ${plainLyrics != null}"
                        )
                        val lyricsData = LyricsData(plainLyrics, syncedLyrics, wordByWordLyrics)
                        return lyricsData
                    }
                }
                } catch (e: Exception) {
                    Log.e(TAG, "LRCLib lyrics fetch failed: ${e.message}", e)
                }
            }

        // No lyrics found from APIs
        return null
    }

    /**
     * Finds local lyrics file in app's files directory OR next to the music file
     * Supports both .lrc files (in music folder) and .json cache files (in app folder)
     */
    private fun findLocalLyrics(artist: String, title: String): LyricsData? {
        // First, check for .lrc file next to the music file
        try {
            val lrcLyrics = findLrcFileForSong(artist, title)
            if (lrcLyrics != null) {
                Log.d(TAG, "Found local .lrc file for: $artist - $title")
                return lrcLyrics
            }
        } catch (e: Exception) {
            Log.w(TAG, "Error checking for .lrc file: ${e.message}")
        }
        
        // Second, check for cached JSON lyrics in app's files directory
        val fileName = "${artist}_${title}.json".replace(Regex("[^a-zA-Z0-9._-]"), "_")
        val file = File(context.filesDir, "lyrics/$fileName")
        return try {
            if (file.exists()) {
                val json = file.readText()
                Gson().fromJson(json, LyricsData::class.java)
            } else {
                null
            }
        } catch (e: Exception) {
            Log.e(TAG, "Error reading local lyrics file: ${e.message}", e)
            null
        }
    }
    
    /**
     * Searches for .lrc file next to the music file
     * Looks for files with same name as the song or generic patterns
     */
    private fun findLrcFileForSong(artist: String, title: String): LyricsData? {
        try {
            // Find the song in MediaStore to get its path
            val projection = arrayOf(MediaStore.Audio.Media._ID, MediaStore.Audio.Media.DATA)
            val selection = "${MediaStore.Audio.Media.TITLE} = ? AND ${MediaStore.Audio.Media.ARTIST} = ?"
            val selectionArgs = arrayOf(title, artist)
            
            context.contentResolver.query(
                MediaStore.Audio.Media.EXTERNAL_CONTENT_URI,
                projection,
                selection,
                selectionArgs,
                null
            )?.use { cursor ->
                if (cursor.moveToFirst()) {
                    val dataIndex = cursor.getColumnIndexOrThrow(MediaStore.Audio.Media.DATA)
                    val songPath = cursor.getString(dataIndex)
                    
                    if (songPath != null) {
                        val songFile = File(songPath)
                        val directory = songFile.parentFile
                        val songNameWithoutExt = songFile.nameWithoutExtension
                        
                        if (directory != null && directory.exists()) {
                            // Look for .lrc file with same name as the song
                            val lrcFile = File(directory, "$songNameWithoutExt.lrc")
                            if (lrcFile.exists() && lrcFile.canRead()) {
                                val lrcContent = lrcFile.readText()
                                return parseLrcFile(lrcContent)
                            }
                            
                            // Also try with artist - title pattern
                            val cleanArtist = artist.replace(Regex("[^a-zA-Z0-9]"), "_")
                            val cleanTitle = title.replace(Regex("[^a-zA-Z0-9]"), "_")
                            val alternativeLrcFile = File(directory, "${cleanArtist}_${cleanTitle}.lrc")
                            if (alternativeLrcFile.exists() && alternativeLrcFile.canRead()) {
                                val lrcContent = alternativeLrcFile.readText()
                                return parseLrcFile(lrcContent)
                            }
                        }
                    }
                }
            }
        } catch (e: Exception) {
            Log.e(TAG, "Error searching for .lrc file", e)
        }
        return null
    }
    
    /**
     * Parses .lrc file content into LyricsData format
     * LRC format: [mm:ss.xx]lyrics text
     */
    private fun parseLrcFile(lrcContent: String): LyricsData? {
        try {
            if (lrcContent.isBlank()) return null
            
            val lines = lrcContent.lines()
            val syncedLines = mutableListOf<String>()
            val plainLines = mutableListOf<String>()
            var hasSyncedLyrics = false
            
            // Pattern to match LRC timestamps [mm:ss.xx] or [mm:ss]
            val timestampPattern = Regex("\\[(\\d{2}):(\\d{2})(?:\\.(\\d{2,3}))?\\](.*)") 
            
            for (line in lines) {
                val trimmedLine = line.trim()
                if (trimmedLine.isEmpty()) continue
                
                // Check if line has timestamp
                val match = timestampPattern.find(trimmedLine)
                if (match != null) {
                    hasSyncedLyrics = true
                    syncedLines.add(trimmedLine) // Keep the timestamp for synced lyrics
                    val lyricsText = match.groupValues[4].trim()
                    if (lyricsText.isNotEmpty()) {
                        plainLines.add(lyricsText) // Extract just the lyrics text for plain version
                    }
                } else {
                    // Metadata line (like [ar:], [ti:], [al:]) or plain text
                    if (!trimmedLine.startsWith("[") || !trimmedLine.contains("]")) {
                        plainLines.add(trimmedLine)
                    }
                }
            }
            
            val plainLyrics = if (plainLines.isNotEmpty()) plainLines.joinToString("\n") else null
            val syncedLyrics = if (hasSyncedLyrics && syncedLines.isNotEmpty()) syncedLines.joinToString("\n") else null
            
            if (plainLyrics != null || syncedLyrics != null) {
                Log.d(TAG, "Successfully parsed .lrc file - Synced: ${syncedLyrics != null}, Plain: ${plainLyrics != null}")
                return LyricsData(plainLyrics, syncedLyrics, null)
            }
        } catch (e: Exception) {
            Log.e(TAG, "Error parsing .lrc file", e)
        }
        return null
    }

    /**
     * Saves lyrics to a local file
     */
    private fun saveLocalLyrics(artist: String, title: String, lyricsData: LyricsData) {
        try {
            val fileName = "${artist}_${title}.json".replace(Regex("[^a-zA-Z0-9._-]"), "_")
            val lyricsDir = File(context.filesDir, "lyrics")
            lyricsDir.mkdirs()

            val file = File(lyricsDir, fileName)
            val json = Gson().toJson(lyricsData)
            file.writeText(json)
            Log.d(TAG, "Saved lyrics to local file: ${file.absolutePath}")
        } catch (e: Exception) {
            Log.e(TAG, "Error saving lyrics to local file: ${e.message}", e)
        }
    }

    /**
     * Fetches album art from Deezer API for albums without artwork
     */
    suspend fun fetchAlbumArtwork(albums: List<Album>): List<Album> = withContext(Dispatchers.IO) {
        val updatedAlbums = mutableListOf<Album>()

        for (album in albums) {
            // Check if the album has a content:// URI and if it actually exists
            if (album.artworkUri != null) {
                if (album.artworkUri.toString()
                        .startsWith("content://media/external/audio/albumart")
                ) {
                    // Try to open the input stream to check if the artwork exists
                    var artworkExists = false
                    try {
                        context.contentResolver.openInputStream(album.artworkUri)?.use {
                            artworkExists = true
                        }
                    } catch (e: Exception) {
                        Log.d(
                            TAG,
                            "Album artwork URI exists but can't be accessed for ${album.title}: ${album.artworkUri}",
                            e
                        )
                        artworkExists = false
                    }

                    if (artworkExists) {
                        updatedAlbums.add(album)
                        continue
                    }
                } else {
                    updatedAlbums.add(album)
                    continue
                }
            }

            // Check cache first
            val cacheKey = "${album.artist}:${album.title}"
            val cachedUri = albumImageCache[cacheKey]
            if (cachedUri != null) {
                updatedAlbums.add(album.copy(artworkUri = cachedUri))
                continue
            }

            try {
                // Skip albums with empty or "Unknown" artist/title
                if (album.artist.isBlank() || album.title.isBlank() ||
                    album.artist.equals("Unknown", ignoreCase = true) ||
                    album.title.equals("Unknown", ignoreCase = true)
                ) {
                    updatedAlbums.add(album)
                    continue
                }

                // Generate a custom placeholder image based on album name
                val placeholderUri = chromahub.rhythm.app.util.ImageUtils.generatePlaceholderImage(
                    name = album.title,
                    size = 500,
                    cacheDir = context.cacheDir
                )

                // Add delay to avoid rate limiting
                delay(500)

                Log.d(TAG, "Searching for album: ${album.title} by ${album.artist}")

                // Search for the album on Deezer (only if enabled)
                if (NetworkClient.isDeezerApiEnabled()) {
                    // Add delay to avoid rate limiting
                    delay(300)
                    
                    try {
                        // First try searching for the album by title and artist
                        val searchQuery = "${album.title} ${album.artist}"
                        var albumSearchResponse = deezerApiService.searchAlbums(searchQuery)
                        var deezerAlbum = findBestAlbumMatch(albumSearchResponse.data, album.title, album.artist)

                        // If no match, try with just the album title
                        if (deezerAlbum == null && album.title.isNotBlank()) {
                            albumSearchResponse = deezerApiService.searchAlbums(album.title)
                            deezerAlbum = findBestAlbumMatch(albumSearchResponse.data, album.title, album.artist)
                        }

                        if (deezerAlbum != null) {
                            // Choose the highest quality image available for album artwork
                            val imageUrl = when {
                                !deezerAlbum.coverXl.isNullOrEmpty() -> deezerAlbum.coverXl
                                !deezerAlbum.coverBig.isNullOrEmpty() -> deezerAlbum.coverBig
                                !deezerAlbum.coverMedium.isNullOrEmpty() -> deezerAlbum.coverMedium
                                !deezerAlbum.cover.isNullOrEmpty() -> deezerAlbum.cover
                                else -> null
                            }

                            if (!imageUrl.isNullOrEmpty() && imageUrl.startsWith("http")) {
                                val imageUri = Uri.parse(imageUrl)
                                albumImageCache[cacheKey] = imageUri
                                updatedAlbums.add(album.copy(artworkUri = imageUri))
                                Log.d(TAG, "Found Deezer album artwork for: ${album.title}, URL: $imageUrl")
                                continue
                            }
                        } else {
                            Log.d(TAG, "No Deezer album found for: ${album.title} by ${album.artist}")
                        }
                    } catch (e: Exception) {
                        Log.w(TAG, "Deezer album search failed for ${album.title}: ${e.message}")
                    }
                }

                // -------- YTMusic fallback (only when local album art is absent) --------
                // Check if we should use YTMusic fallback for albums
                if (NetworkClient.isYTMusicApiEnabled()) {
                    var foundAlbumArt = false
                    try {
                        Log.d(TAG, "Trying YTMusic for album: ${album.title} by ${album.artist}")
                        
                        // Create search request for album
                        val searchQuery = "${album.title} ${album.artist}"
                    val searchRequest = YTMusicSearchRequest(
                        context = YTMusicContext(YTMusicClient()),
                        query = searchQuery,
                        params = "EgWKAQIYAWoKEAoQAxAEEAkQBQ%3D%3D" // Album search filter
                    )
                    
                    val searchResponse = ytmusicApiService.search(request = searchRequest)
                    if (searchResponse.isSuccessful) {
                        val imageUrl = searchResponse.body()?.extractAlbumImageUrl()
                        if (!imageUrl.isNullOrEmpty()) {
                            val imageUri = Uri.parse(imageUrl)
                            albumImageCache[cacheKey] = imageUri
                            updatedAlbums.add(album.copy(artworkUri = imageUri))
                            Log.d(TAG, "Found YTMusic album art for ${album.title}: $imageUrl")
                            foundAlbumArt = true
                        } else {
                            // Try to get detailed album info for better quality cover art
                            val browseId = searchResponse.body()?.extractAlbumBrowseId()
                            if (!browseId.isNullOrEmpty()) {
                                val browseRequest = YTMusicBrowseRequest(
                                    context = YTMusicContext(YTMusicClient()),
                                    browseId = browseId
                                )
                                val albumResponse = ytmusicApiService.getAlbum(request = browseRequest)
                                if (albumResponse.isSuccessful) {
                                    val detailedImageUrl = albumResponse.body()?.extractAlbumCover()
                                    if (!detailedImageUrl.isNullOrEmpty()) {
                                        val imageUri = Uri.parse(detailedImageUrl)
                                        albumImageCache[cacheKey] = imageUri
                                        updatedAlbums.add(album.copy(artworkUri = imageUri))
                                        Log.d(TAG, "Found detailed YTMusic album art for ${album.title}: $detailedImageUrl")
                                        foundAlbumArt = true
                                    }
                                }
                            }
                        }
                    }
                    } catch (e: Exception) {
                        Log.w(TAG, "YTMusic fallback failed for album ${album.title}: ${e.message}")
                    }

                    // If YTMusic didn't find anything, use placeholder
                    if (!foundAlbumArt) {
                        Log.d(
                            TAG,
                            "No valid image found for album: ${album.title}, using generated placeholder"
                        )
                        albumImageCache[cacheKey] = placeholderUri
                        updatedAlbums.add(album.copy(artworkUri = placeholderUri))
                    }
                } else {
                    // YTMusic is disabled, use placeholder
                    Log.d(
                        TAG,
                        "No valid image found for album: ${album.title}, using generated placeholder"
                    )
                    albumImageCache[cacheKey] = placeholderUri
                    updatedAlbums.add(album.copy(artworkUri = placeholderUri))
                }

            } catch (e: Exception) {
                Log.e(TAG, "Error fetching album artwork for ${album.title}", e)
                // Try to use a generated placeholder
                try {
                    val placeholderUri =
                        chromahub.rhythm.app.util.ImageUtils.generatePlaceholderImage(
                            name = album.title,
                            size = 500,
                            cacheDir = context.cacheDir
                        )
                    updatedAlbums.add(album.copy(artworkUri = placeholderUri))
                } catch (e2: Exception) {
                    // If even placeholder generation fails, keep the original album without changes
                    updatedAlbums.add(album)
                }
            }
        }

        updatedAlbums
    }

    /**
     * Fetches track/song images from YTMusic API for songs without artwork.
     * This is used as a fallback when local album art is absent and other APIs fail.
     */
    suspend fun fetchTrackArtwork(songs: List<Song>): List<Song> = withContext(Dispatchers.IO) {
        val updatedSongs = mutableListOf<Song>()

        for (song in songs) {
            // Only fetch if song doesn't have artwork already
            if (song.artworkUri != null) {
                updatedSongs.add(song)
                continue
            }

            // Check if album has artwork - if yes, we don't need track-specific artwork
            if (song.albumId.isNotBlank()) {
                val albums = loadAlbums()
                val album = albums.find { it.id == song.albumId }
                if (album?.artworkUri != null) {
                    // Album has artwork, no need for track-specific image
                    updatedSongs.add(song)
                    continue
                }
            }

            val cacheKey = "${song.artist}:${song.title}"
            
            // Check cache first
            val cachedUri = albumImageCache[cacheKey] // Reuse album cache for tracks
            if (cachedUri != null) {
                updatedSongs.add(song.copy(artworkUri = cachedUri))
                continue
            }

            try {
                // Skip songs with empty or "Unknown" artist/title
                if (song.artist.isBlank() || song.title.isBlank() ||
                    song.artist.equals("Unknown", ignoreCase = true) ||
                    song.title.equals("Unknown", ignoreCase = true)
                ) {
                    updatedSongs.add(song)
                    continue
                }

                if (!NetworkClient.isYTMusicApiEnabled()) {
                    Log.d(TAG, "YTMusic API is disabled, skipping track artwork for: ${song.title}")
                    updatedSongs.add(song)
                    continue
                }

                Log.d(TAG, "Searching YTMusic for track: ${song.title} by ${song.artist}")

                // Add delay to avoid rate limiting
                delay(200)

                // Create search request for song/track
                val searchQuery = "${song.title} ${song.artist}"
                val searchRequest = YTMusicSearchRequest(
                    context = YTMusicContext(YTMusicClient()),
                    query = searchQuery,
                    params = "EgWKAQIIAWoKEAoQAxAEEAkQBQ%3D%3D" // Song search filter
                )

                val searchResponse = ytmusicApiService.search(request = searchRequest)
                if (searchResponse.isSuccessful) {
                    // For tracks, we can extract image from the first result
                    val imageUrl = searchResponse.body()?.extractAlbumImageUrl() // Tracks use same thumbnail structure
                    if (!imageUrl.isNullOrEmpty()) {
                        val imageUri = Uri.parse(imageUrl)
                        albumImageCache[cacheKey] = imageUri // Cache for future use
                        updatedSongs.add(song.copy(artworkUri = imageUri))
                        Log.d(TAG, "Found YTMusic track image for ${song.title}: $imageUrl")
                        continue
                    }
                }

                Log.d(TAG, "No YTMusic image found for track: ${song.title}")
                updatedSongs.add(song)

            } catch (e: Exception) {
                Log.w(TAG, "YTMusic track image fetch failed for ${song.title}: ${e.message}")
                updatedSongs.add(song)
            }
        }

        updatedSongs
    }

    suspend fun getSongsForAlbum(albumId: String): List<Song> = withContext(Dispatchers.IO) {
        val allSongs = loadSongs() // Ensure songs are loaded once
        val allAlbums = loadAlbums() // Ensure albums are loaded once

        Log.d("MusicRepository", "Getting songs for album ID: $albumId")

        // Find the album by ID
        val album = allAlbums.find { it.id == albumId }
        if (album == null) {
            Log.e("MusicRepository", "Album not found with ID: $albumId")
            return@withContext emptyList()
        }

        Log.d("MusicRepository", "Found album: ${album.title} (ID: $albumId)")

        // Filter songs that match the album's title and ID
        val albumSongs = allSongs.filter { song ->
            val albumTitleMatch = song.album == album.title
            val albumIdMatch = song.albumId == albumId
            albumTitleMatch && albumIdMatch
        }

        Log.d("MusicRepository", "Found ${albumSongs.size} songs for album: ${album.title}")

        if (albumSongs.isEmpty()) {
            Log.d(
                "MusicRepository",
                "No songs found for album title: ${album.title} and ID: $albumId, trying direct lookup"
            )
            return@withContext loadSongsForAlbumDirect(albumId)
        }

        return@withContext albumSongs
    }

    private suspend fun loadSongsForAlbumDirect(albumId: String): List<Song> =
        withContext(Dispatchers.IO) {
            val songs = mutableListOf<Song>()
            val collection = MediaStore.Audio.Media.EXTERNAL_CONTENT_URI

            val selection = "${MediaStore.Audio.Media.ALBUM_ID} = ?"
            val selectionArgs = arrayOf(albumId)
            val sortOrder = "${MediaStore.Audio.Media.TRACK} ASC"

            Log.d("MusicRepository", "Querying MediaStore directly for album ID: $albumId")

            context.contentResolver.query(
                collection,
                null, // Get all columns
                selection,
                selectionArgs,
                sortOrder
            )?.use { cursor ->
                Log.d("MusicRepository", "Found ${cursor.count} songs for album ID: $albumId")

                // Cache column indices
                val idColumn = cursor.getColumnIndexOrThrow(MediaStore.Audio.Media._ID)
                val titleColumn = cursor.getColumnIndexOrThrow(MediaStore.Audio.Media.TITLE)
                val artistColumn = cursor.getColumnIndexOrThrow(MediaStore.Audio.Media.ARTIST)
                val albumColumn = cursor.getColumnIndexOrThrow(MediaStore.Audio.Media.ALBUM)
                val albumIdColumn = cursor.getColumnIndexOrThrow(MediaStore.Audio.Media.ALBUM_ID)
                val durationColumn = cursor.getColumnIndexOrThrow(MediaStore.Audio.Media.DURATION)
                val trackColumn = cursor.getColumnIndexOrThrow(MediaStore.Audio.Media.TRACK)
                val yearColumn = cursor.getColumnIndexOrThrow(MediaStore.Audio.Media.YEAR)

                while (cursor.moveToNext()) {
                    val id = cursor.getLong(idColumn)
                    val title = cursor.getString(titleColumn)
                    val artist = cursor.getString(artistColumn)
                    val album = cursor.getString(albumColumn)
                    val retrievedAlbumId = cursor.getLong(albumIdColumn)
                    val duration = cursor.getLong(durationColumn)
                    val track = cursor.getInt(trackColumn)
                    val year = cursor.getInt(yearColumn)

                    Log.d(
                        "MusicRepository",
                        "Found song: $title for album: $album (ID: $retrievedAlbumId)"
                    )

                    val contentUri: Uri = ContentUris.withAppendedId(
                        MediaStore.Audio.Media.EXTERNAL_CONTENT_URI,
                        id
                    )

                    val albumArtUri = ContentUris.withAppendedId(
                        Uri.parse("content://media/external/audio/albumart"),
                        retrievedAlbumId
                    )

                    val song = Song(
                        id = id.toString(),
                        title = title,
                        artist = artist,
                        album = album,
                        albumId = retrievedAlbumId.toString(),
                        duration = duration,
                        uri = contentUri,
                        artworkUri = albumArtUri,
                        trackNumber = track,
                        year = year
                    )
                    songs.add(song)
                }
            }

            songs
        }

    suspend fun getSongsForArtist(artistId: String): List<Song> = withContext(Dispatchers.IO) {
        val allSongs = loadSongs() // Ensure songs are loaded once
        val allArtists = loadArtists() // Ensure artists are loaded once
        val appSettings = AppSettings.getInstance(context)
        val groupByAlbumArtist = appSettings.groupByAlbumArtist.value

        Log.d("MusicRepository", "Getting songs for artist ID: $artistId")

        // Find the artist by ID
        val artist = allArtists.find { it.id == artistId }
        if (artist == null) {
            Log.e("MusicRepository", "Artist not found with ID: $artistId")
            return@withContext emptyList()
        }

        Log.d("MusicRepository", "Found artist: ${artist.name} (ID: $artistId, groupByAlbumArtist=$groupByAlbumArtist)")

        // Filter songs that match the artist's name
        // When grouping by album artist, check album artist first, then fall back to track artist
        val artistSongs = allSongs.filter { song ->
            if (groupByAlbumArtist) {
                val songArtistName = (song.albumArtist?.takeIf { it.isNotBlank() } ?: song.artist).trim()
                songArtistName == artist.name
            } else {
                song.artist == artist.name
            }
        }

        Log.d("MusicRepository", "Found ${artistSongs.size} songs for artist: ${artist.name}")
        return@withContext artistSongs
    }

    suspend fun getAlbumsForArtist(artistId: String): List<Album> = withContext(Dispatchers.IO) {
        val allAlbums = loadAlbums() // Ensure albums are loaded once
        val allArtists = loadArtists() // Ensure artists are loaded once
        val allSongs = loadSongs() // Need songs to check album artist
        val appSettings = AppSettings.getInstance(context)
        val groupByAlbumArtist = appSettings.groupByAlbumArtist.value

        Log.d("MusicRepository", "Getting albums for artist ID: $artistId")

        // Find the artist by ID
        val artist = allArtists.find { it.id == artistId }
        if (artist == null) {
            Log.e("MusicRepository", "Artist not found with ID: $artistId")
            return@withContext emptyList()
        }

        Log.d("MusicRepository", "Found artist: ${artist.name} (ID: $artistId, groupByAlbumArtist=$groupByAlbumArtist)")

        // Filter albums that match the artist's name
        // When grouping by album artist, check if any song in the album has matching album artist
        val artistAlbums = allAlbums.filter { album ->
            if (groupByAlbumArtist) {
                // Check if any song from this album has the artist as album artist
                allSongs.any { song ->
                    song.album == album.title &&
                    song.albumId == album.id &&
                    (song.albumArtist?.takeIf { it.isNotBlank() } ?: song.artist).trim() == artist.name
                }
            } else {
                album.artist == artist.name
            }
        }

        Log.d("MusicRepository", "Found ${artistAlbums.size} albums for artist: ${artist.name}")
        return@withContext artistAlbums
    }

    suspend fun createPlaylist(name: String): Playlist {
        return Playlist(
            id = System.currentTimeMillis().toString(),
            name = name
        )
    }

    // Mock data for locations
    fun getLocations(): List<PlaybackLocation> {
        return listOf(
            PlaybackLocation(
                id = "living_room",
                name = "Living room",
                icon = 0 // Replace with actual icon resource
            ),
            PlaybackLocation(
                id = "bedroom",
                name = "Bedroom",
                icon = 0 // Replace with actual icon resource
            ),
            PlaybackLocation(
                id = "kitchen",
                name = "Kitchen",
                icon = 0 // Replace with actual icon resource
            )
        )
    }

    /**
     * Checks if the device is currently connected to the internet
     */
    fun isNetworkAvailable(): Boolean {
        val connectivityManager =
            context.getSystemService(Context.CONNECTIVITY_SERVICE) as android.net.ConnectivityManager

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            val network = connectivityManager.activeNetwork ?: return false
            val capabilities = connectivityManager.getNetworkCapabilities(network) ?: return false

            return capabilities.hasCapability(android.net.NetworkCapabilities.NET_CAPABILITY_INTERNET)
        } else {
            @Suppress("DEPRECATION")
            val networkInfo = connectivityManager.activeNetworkInfo ?: return false
            @Suppress("DEPRECATION")
            return networkInfo.isConnected
        }
    }

    /**
     * Finds locally stored artist image
     */
    private fun findLocalArtistImage(artistName: String): Uri? {
        val fileName = "${artistName}.jpg".replace(Regex("[^a-zA-Z0-9._-]"), "_")
        val file = File(context.filesDir, "artist_images/$fileName")
        return if (file.exists()) Uri.fromFile(file) else null
    }

    /**
     * Saves artist image to local storage
     */
    private fun saveLocalArtistImage(artistName: String, imageUrl: String) {
        try {
            val fileName = "${artistName}.jpg".replace(Regex("[^a-zA-Z0-9._-]"), "_")
            val imageDir = File(context.filesDir, "artist_images")
            imageDir.mkdirs()

            val file = File(imageDir, fileName)
            URL(imageUrl).openStream().use { input ->
                file.outputStream().use { output ->
                    input.copyTo(output)
                }
            }
            Log.d(TAG, "Saved artist image to local file: ${file.absolutePath}")
        } catch (e: Exception) {
            Log.e(TAG, "Error saving artist image to local file: ${e.message}", e)
        }
    }
    
    /**
     * Clears all in-memory caches
     */
    fun clearInMemoryCaches() {
        try {
            synchronized(artistImageCache) {
                artistImageCache.clear()
            }
            synchronized(albumImageCache) {
                albumImageCache.clear()
            }
            synchronized(lyricsCache) {
                lyricsCache.clear()
            }
            Log.d(TAG, "Cleared all in-memory caches (artist images, album images, lyrics)")
        } catch (e: Exception) {
            Log.e(TAG, "Error clearing in-memory caches", e)
        }
    }
    
    /**
     * Performs cache maintenance - removes expired entries and optimizes memory usage
     */
    suspend fun performCacheMaintenance() = withContext(Dispatchers.IO) {
        try {
            Log.d(TAG, "Starting cache maintenance...")
            
            // Check if cache cleanup is needed based on app settings
            val maxCacheSize = try {
                val settings = chromahub.rhythm.app.data.AppSettings.getInstance(context)
                settings.maxCacheSize.value
            } catch (e: Exception) {
                Log.w(TAG, "Error getting cache size setting, using default", e)
                512L * 1024L * 1024L // Default 512MB
            }
            
            // Clean up file system cache if needed
            val cacheManager = chromahub.rhythm.app.util.CacheManager
            val cacheCleanedUp = cacheManager.cleanCacheIfNeeded(context, maxCacheSize)
            
            if (cacheCleanedUp) {
                Log.d(TAG, "File system cache was cleaned up")
            }
            
            // Optimize in-memory caches
            val initialArtistCacheSize = artistImageCache.size
            val initialAlbumCacheSize = albumImageCache.size
            val initialLyricsCacheSize = lyricsCache.size
            
            // The LinkedHashMap LRU implementation will automatically evict oldest entries
            // when new entries are added and the cache exceeds its limit
            
            Log.d(TAG, "Cache maintenance completed. " +
                    "Artist cache: $initialArtistCacheSize entries, " +
                    "Album cache: $initialAlbumCacheSize entries, " +
                    "Lyrics cache: $initialLyricsCacheSize entries")
                    
        } catch (e: Exception) {
            Log.e(TAG, "Error during cache maintenance", e)
        }
    }
    
    /**
     * Gets the current size of in-memory caches
     * @return A map containing cache names and their sizes
     */
    fun getInMemoryCacheInfo(): Map<String, Int> {
        return mapOf(
            "artistImageCache" to artistImageCache.size,
            "albumImageCache" to albumImageCache.size,
            "lyricsCache" to lyricsCache.size
        )
    }
    
    /**
     * Finds the best matching Deezer artist from search results using fuzzy matching
     */
    private fun findBestMatch(artists: List<DeezerArtist>, originalName: String): DeezerArtist? {
        if (artists.isEmpty()) return null
        
        val lowerOriginal = originalName.lowercase().trim()
        
        // First, try exact match (case insensitive)
        artists.find { it.name.lowercase().trim() == lowerOriginal }?.let { return it }
        
        // Second, try starts with match
        artists.find { it.name.lowercase().trim().startsWith(lowerOriginal) }?.let { return it }
        
        // Third, try contains match
        artists.find { it.name.lowercase().contains(lowerOriginal) }?.let { return it }
        
        // Fourth, try reversed contains (original contains artist name)
        artists.find { lowerOriginal.contains(it.name.lowercase().trim()) }?.let { return it }
        
        // Fifth, try word-by-word matching
        val originalWords = lowerOriginal.split(Regex("\\s+")).filter { it.isNotEmpty() }
        if (originalWords.isNotEmpty()) {
            artists.find { artist ->
                val artistWords = artist.name.lowercase().split(Regex("\\s+"))
                originalWords.any { originalWord ->
                    artistWords.any { artistWord ->
                        originalWord == artistWord || 
                        originalWord.startsWith(artistWord) || 
                        artistWord.startsWith(originalWord)
                    }
                }
            }?.let { return it }
        }
        
        // Finally, return the first result with the most fans (most popular)
        return artists.maxByOrNull { it.nbFan }
    }
    
    /**
     * Finds the best matching Deezer album from search results using fuzzy matching
     */
    private fun findBestAlbumMatch(albums: List<DeezerAlbum>, originalTitle: String, originalArtist: String): DeezerAlbum? {
        if (albums.isEmpty()) return null

        val lowerTitle = originalTitle.lowercase().trim()
        val lowerArtist = originalArtist.lowercase().trim()

        // First, try exact match (case insensitive) for both title and artist
        albums.find { album ->
            album.title.lowercase().trim() == lowerTitle &&
            (album.artist?.name?.lowercase()?.trim() == lowerArtist || lowerArtist.contains(album.artist?.name?.lowercase()?.trim() ?: ""))
        }?.let { return it }

        // Second, try exact title match with fuzzy artist match
        albums.find { album ->
            album.title.lowercase().trim() == lowerTitle
        }?.let { return it }

        // Third, try contains match for title
        albums.find { album ->
            album.title.lowercase().contains(lowerTitle) || lowerTitle.contains(album.title.lowercase())
        }?.let { return it }

        // Fourth, try word-by-word matching for title
        val titleWords = lowerTitle.split(Regex("\\s+")).filter { it.isNotEmpty() }
        if (titleWords.isNotEmpty()) {
            albums.find { album ->
                val albumWords = album.title.lowercase().split(Regex("\\s+"))
                titleWords.any { titleWord ->
                    albumWords.any { albumWord ->
                        titleWord == albumWord ||
                        titleWord.startsWith(albumWord) ||
                        albumWord.startsWith(titleWord)
                    }
                }
            }?.let { return it }
        }

        // Finally, return the first result with the most tracks (most complete album)
        return albums.maxByOrNull { it.nbTracks }
    }

    /**
     * Detects genres for songs in background after initial app load
     * This method processes songs in batches to avoid blocking the UI
     * @param songs List of songs to detect genres for
     * @param onProgress Callback to report progress (current, total)
     * @param onComplete Callback when genre detection is complete
     */
    suspend fun detectGenresInBackground(
        songs: List<Song>,
        onProgress: ((Int, Int) -> Unit)? = null,
        onComplete: ((List<Song>) -> Unit)? = null
    ) = withContext(Dispatchers.IO) {
        val songsWithoutGenres = songs.filter { it.genre == null }
        if (songsWithoutGenres.isEmpty()) {
            Log.d(TAG, "All songs already have genres, skipping background detection")
            onComplete?.invoke(songs)
            return@withContext
        }

        Log.d(TAG, "Starting background genre detection for ${songsWithoutGenres.size} songs")
        val updatedSongs = mutableListOf<Song>()
        val batchSize = 50 // Process in smaller batches for better responsiveness
        var processedCount = 0

        songsWithoutGenres.chunked(batchSize).forEach { batch ->
            val batchStartTime = System.currentTimeMillis()

            batch.forEach { song ->
                try {
                    val songId = song.id.toLongOrNull() ?: return@forEach
                    val contentUri = song.uri
                    val genre = getGenreForSong(context, contentUri, songId.toInt())

                    if (genre != null) {
                        val updatedSong = song.copy(genre = genre)
                        updatedSongs.add(updatedSong)
                        Log.d(TAG, "Detected genre '$genre' for song: ${song.title}")
                    } else {
                        // Keep the original song if no genre was found
                        updatedSongs.add(song)
                    }

                    processedCount++
                    onProgress?.invoke(processedCount, songsWithoutGenres.size)

                } catch (e: Exception) {
                    Log.w(TAG, "Error detecting genre for song ${song.title}", e)
                    updatedSongs.add(song) // Keep original song on error
                    processedCount++
                    onProgress?.invoke(processedCount, songsWithoutGenres.size)
                }
            }

            val batchEndTime = System.currentTimeMillis()
            val batchDuration = batchEndTime - batchStartTime
            Log.d(TAG, "Processed batch of ${batch.size} songs in ${batchDuration}ms")

            // Yield control to allow other coroutines to run
            yield()

            // Small delay between batches to prevent overwhelming the system
            if (batchDuration < 100) { // If batch processed quickly, add a small delay
                delay(50)
            }
        }

        val finalSongs = songs.map { originalSong ->
            updatedSongs.find { it.id == originalSong.id } ?: originalSong
        }

        Log.d(TAG, "Background genre detection complete. Updated ${updatedSongs.size} songs with genres")
        onComplete?.invoke(finalSongs)
    }
    
    /**
     * Cleanup method to clear caches and cancel coroutines
     * Call this when the repository is no longer needed
     */
    fun cleanup() {
        Log.d(TAG, "Cleaning up MusicRepository...")
        
        // Cancel all coroutines
        repositoryScope.coroutineContext[kotlinx.coroutines.Job]?.cancel()
        
        // Clear all caches
        artistImageCache.clear()
        albumImageCache.clear()
        lyricsCache.clear()
        
        // Clear rate limiting maps
        lastApiCalls.clear()
        apiCallCounts.clear()
        
        Log.d(TAG, "MusicRepository cleaned up")
    }
}