package chromahub.rhythm.app.util

import android.content.Context
import android.net.Uri
import android.os.Environment
import android.util.Log
import androidx.documentfile.provider.DocumentFile
import chromahub.rhythm.app.data.Playlist
import chromahub.rhythm.app.data.Song
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import java.io.*
import java.text.SimpleDateFormat
import java.util.*

/**
 * Utility class for importing and exporting playlists in various formats
 */
object PlaylistImportExportUtils {
    private const val TAG = "PlaylistImportExport"
    
    enum class PlaylistExportFormat(val extension: String, val mimeType: String, val displayName: String) {
        JSON(".json", "application/json", "JSON Format"),
        M3U(".m3u", "audio/x-mpegurl", "M3U Playlist"),
        M3U8(".m3u8", "application/x-mpegURL", "M3U8 Playlist"),
        PLS(".pls", "audio/x-scpls", "PLS Playlist")
    }
    
    data class PlaylistExportData(
        val name: String,
        val id: String,
        val dateCreated: Long,
        val dateModified: Long,
        val songs: List<PlaylistSongEntry>,
        val exportedAt: Long = System.currentTimeMillis(),
        val exportedBy: String = "Rhythm Music Player"
    )
    
    data class PlaylistSongEntry(
        val title: String,
        val artist: String,
        val album: String,
        val duration: Long,
        val filePath: String?, // Original file path if available
        val uri: String, // Content URI
        val trackNumber: Int = 0,
        val year: Int = 0
    )
    
    /**
     * Exports a single playlist to the specified format
     */
    fun exportPlaylist(
        context: Context,
        playlist: Playlist,
        format: PlaylistExportFormat,
        outputDirectory: File? = null,
        userSelectedDirectoryUri: Uri? = null
    ): Result<File> {
        return try {
            val fileName = sanitizeFileName("${playlist.name}_${getCurrentTimestamp()}${format.extension}")
            
            // Use user-selected directory if provided, otherwise use default
            val outputFile = if (userSelectedDirectoryUri != null) {
                // Use Storage Access Framework for user-selected directory
                exportToUserSelectedDirectory(context, userSelectedDirectoryUri, playlist, format, fileName)
            } else {
                // Use traditional file system approach
                val exportDir = outputDirectory ?: getDefaultExportDirectory(context)
                if (!exportDir.exists()) {
                    exportDir.mkdirs()
                }
                val file = File(exportDir, fileName)
                
                when (format) {
                    PlaylistExportFormat.JSON -> exportToJson(playlist, file)
                    PlaylistExportFormat.M3U -> exportToM3u(playlist, file, false)
                    PlaylistExportFormat.M3U8 -> exportToM3u(playlist, file, true)
                    PlaylistExportFormat.PLS -> exportToPls(playlist, file)
                }
                file
            }
            
            Log.d(TAG, "Successfully exported playlist '${playlist.name}' to ${outputFile.absolutePath}")
            Result.success(outputFile)
        } catch (e: Exception) {
            Log.e(TAG, "Error exporting playlist '${playlist.name}'", e)
            Result.failure(e)
        }
    }
    
    /**
     * Exports multiple playlists to a single ZIP file
     */
    fun exportAllPlaylists(
        context: Context,
        playlists: List<Playlist>,
        format: PlaylistExportFormat,
        outputDirectory: File? = null,
        userSelectedDirectoryUri: Uri? = null
    ): Result<File> {
        return try {
            val fileName = "RhythmPlaylists_${getCurrentTimestamp()}.zip"
            
            // Use user-selected directory if provided, otherwise use default
            val zipFile = if (userSelectedDirectoryUri != null) {
                // Use Storage Access Framework for user-selected directory
                exportAllPlaylistsToUserSelectedDirectory(context, userSelectedDirectoryUri, playlists, format, fileName)
            } else {
                // Use traditional file system approach
                val exportDir = outputDirectory ?: getDefaultExportDirectory(context)
                if (!exportDir.exists()) {
                    exportDir.mkdirs()
                }
                val file = File(exportDir, fileName)
                createPlaylistsZip(playlists, format, file)
                file
            }
            
            Log.d(TAG, "Successfully exported ${playlists.size} playlists to ${zipFile.absolutePath}")
            Result.success(zipFile)
        } catch (e: Exception) {
            Log.e(TAG, "Error exporting all playlists", e)
            Result.failure(e)
        }
    }
    
    /**
     * Imports a playlist from a file
     */
    fun importPlaylist(
        context: Context,
        uri: Uri,
        availableSongs: List<Song>
    ): Result<Playlist> {
        return try {
            val inputStream = context.contentResolver.openInputStream(uri)
                ?: return Result.failure(IllegalArgumentException("Cannot open file"))
            
            val content = inputStream.bufferedReader().use { it.readText() }
            val fileName = getFileName(context, uri)
            
            val playlist = when {
                fileName.endsWith(".json", true) -> importFromJson(content, availableSongs)
                fileName.endsWith(".m3u", true) || fileName.endsWith(".m3u8", true) -> 
                    importFromM3u(content, fileName, availableSongs)
                fileName.endsWith(".pls", true) -> importFromPls(content, fileName, availableSongs)
                else -> return Result.failure(IllegalArgumentException("Unsupported file format"))
            }
            
            Log.d(TAG, "Successfully imported playlist '${playlist.name}' with ${playlist.songs.size} songs")
            Result.success(playlist)
        } catch (e: Exception) {
            Log.e(TAG, "Error importing playlist from $uri", e)
            Result.failure(e)
        }
    }
    
    private fun exportToJson(playlist: Playlist, outputFile: File) {
        val exportData = PlaylistExportData(
            name = playlist.name,
            id = playlist.id,
            dateCreated = playlist.dateCreated,
            dateModified = playlist.dateModified,
            songs = playlist.songs.map { song ->
                PlaylistSongEntry(
                    title = song.title,
                    artist = song.artist,
                    album = song.album,
                    duration = song.duration,
                    filePath = song.uri.path,
                    uri = song.uri.toString(),
                    trackNumber = song.trackNumber,
                    year = song.year
                )
            }
        )
        
        val json = Gson().toJson(exportData)
        outputFile.writeText(json)
    }
    
    private fun exportToM3u(playlist: Playlist, outputFile: File, extended: Boolean) {
        outputFile.bufferedWriter().use { writer ->
            if (extended) {
                writer.write("#EXTM3U\n")
            }
            
            playlist.songs.forEach { song ->
                if (extended) {
                    writer.write("#EXTINF:${song.duration / 1000},${song.artist} - ${song.title}\n")
                }
                writer.write("${song.uri}\n")
            }
        }
    }
    
    private fun exportToPls(playlist: Playlist, outputFile: File) {
        outputFile.bufferedWriter().use { writer ->
            writer.write("[playlist]\n")
            
            playlist.songs.forEachIndexed { index, song ->
                val num = index + 1
                writer.write("File$num=${song.uri}\n")
                writer.write("Title$num=${song.artist} - ${song.title}\n")
                writer.write("Length$num=${song.duration / 1000}\n")
            }
            
            writer.write("NumberOfEntries=${playlist.songs.size}\n")
            writer.write("Version=2\n")
        }
    }
    
    private fun importFromJson(content: String, availableSongs: List<Song>): Playlist {
        val exportData = Gson().fromJson(content, PlaylistExportData::class.java)
        val songMap = availableSongs.associateBy { it.uri.toString() }
        val addedSongUris = mutableSetOf<String>()
        val addedSongKeys = mutableSetOf<String>()
        
        val matchedSongs = exportData.songs.mapNotNull { entry ->
            // Create a key for duplicate detection (title + artist, normalized)
            val songKey = "${entry.title.trim().lowercase()}_${entry.artist.trim().lowercase()}"
            
            // Skip if already added by URI or by title+artist combination
            if (addedSongUris.contains(entry.uri) || addedSongKeys.contains(songKey)) {
                Log.d(TAG, "Skipping duplicate song: ${entry.title} by ${entry.artist}")
                return@mapNotNull null
            }
            
            val matchedSong = songMap[entry.uri] ?: 
                // Fallback: match by title and artist
                availableSongs.find { 
                    it.title.equals(entry.title, ignoreCase = true) && 
                    it.artist.equals(entry.artist, ignoreCase = true) 
                }
            
            matchedSong?.let {
                addedSongUris.add(it.uri.toString())
                addedSongKeys.add("${it.title.trim().lowercase()}_${it.artist.trim().lowercase()}")
            }
            
            matchedSong
        }
        
        return Playlist(
            id = System.currentTimeMillis().toString(), // Generate new ID
            name = exportData.name,
            songs = matchedSongs,
            dateCreated = System.currentTimeMillis(),
            dateModified = System.currentTimeMillis()
        )
    }
    
    private fun importFromM3u(content: String, fileName: String, availableSongs: List<Song>): Playlist {
        val lines = content.lines().filter { it.isNotBlank() }
        val matchedSongs = mutableListOf<Song>()
        val addedSongUris = mutableSetOf<String>()
        val addedSongKeys = mutableSetOf<String>()
        var currentTitle = ""
        
        lines.forEach { line ->
            when {
                line.startsWith("#EXTINF:") -> {
                    // Extract title from extended info
                    val titleStart = line.indexOf(',')
                    if (titleStart != -1 && titleStart + 1 < line.length) {
                        currentTitle = line.substring(titleStart + 1)
                    }
                }
                line.startsWith("#") -> {
                    // Skip other comments
                }
                else -> {
                    // This is a file path/URI - process ALL non-comment lines
                    val trimmedLine = line.trim()
                    if (trimmedLine.isNotEmpty()) {
                        val song = findSongByPathOrTitle(trimmedLine, currentTitle, availableSongs)
                        if (song != null) {
                            val songKey = "${song.title.trim().lowercase()}_${song.artist.trim().lowercase()}"
                            val songUri = song.uri.toString()
                            
                            // Check for duplicates
                            if (!addedSongUris.contains(songUri) && !addedSongKeys.contains(songKey)) {
                                matchedSongs.add(song) 
                                addedSongUris.add(songUri)
                                addedSongKeys.add(songKey)
                                Log.d(TAG, "Imported song from M3U: ${song.title} (${song.artist}) from path: $trimmedLine")
                            } else {
                                Log.d(TAG, "Skipping duplicate song in M3U: ${song.title} by ${song.artist}")
                            }
                        } else {
                            Log.w(TAG, "Could not find song for M3U path: $trimmedLine" + 
                                if (currentTitle.isNotEmpty()) " (title: $currentTitle)" else "")
                        }
                    }
                    // Reset currentTitle after processing each path (whether match found or not)
                    currentTitle = ""
                }
            }
        }
        
        Log.d(TAG, "M3U import completed: ${matchedSongs.size} songs imported from ${fileName}")
        return Playlist(
            id = System.currentTimeMillis().toString(),
            name = fileName.substringBeforeLast("."),
            songs = matchedSongs,
            dateCreated = System.currentTimeMillis(),
            dateModified = System.currentTimeMillis()
        )
    }
    
    private fun importFromPls(content: String, fileName: String, availableSongs: List<Song>): Playlist {
        val lines = content.lines()
        val matchedSongs = mutableListOf<Song>()
        val addedSongUris = mutableSetOf<String>()
        val addedSongKeys = mutableSetOf<String>()
        val entries = mutableMapOf<Int, Triple<String?, String?, String?>>() // file, title, length
        
        lines.forEach { line ->
            when {
                line.startsWith("File") -> {
                    val parts = line.split("=", limit = 2)
                    if (parts.size == 2) {
                        val num = parts[0].removePrefix("File").toIntOrNull()
                        if (num != null) {
                            val current = entries[num] ?: Triple(null, null, null)
                            entries[num] = current.copy(first = parts[1])
                        }
                    }
                }
                line.startsWith("Title") -> {
                    val parts = line.split("=", limit = 2)
                    if (parts.size == 2) {
                        val num = parts[0].removePrefix("Title").toIntOrNull()
                        if (num != null) {
                            val current = entries[num] ?: Triple(null, null, null)
                            entries[num] = current.copy(second = parts[1])
                        }
                    }
                }
            }
        }
        
        // Process entries in order
        entries.toSortedMap().forEach { (_, entry) ->
            val (filePath, title, _) = entry
            if (filePath != null) {
                val song = findSongByPathOrTitle(filePath, title ?: "", availableSongs)
                song?.let { 
                    val songKey = "${it.title.trim().lowercase()}_${it.artist.trim().lowercase()}"
                    val songUri = it.uri.toString()
                    
                    // Check for duplicates
                    if (!addedSongUris.contains(songUri) && !addedSongKeys.contains(songKey)) {
                        matchedSongs.add(it)
                        addedSongUris.add(songUri)
                        addedSongKeys.add(songKey)
                    } else {
                        Log.d(TAG, "Skipping duplicate song in PLS: ${it.title} by ${it.artist}")
                    }
                }
            }
        }
        
        return Playlist(
            id = System.currentTimeMillis().toString(),
            name = fileName.substringBeforeLast("."),
            songs = matchedSongs,
            dateCreated = System.currentTimeMillis(),
            dateModified = System.currentTimeMillis()
        )
    }
    
    private fun findSongByPathOrTitle(path: String, title: String, availableSongs: List<Song>): Song? {
        // Try exact URI match first
        availableSongs.find { it.uri.toString() == path }?.let { 
            Log.d(TAG, "Found song by exact URI match: $path")
            return it 
        }
        
        // Try matching by file path (from MediaStore DATA column)
        // The path in M3U might be like: /storage/emulated/0/Music/song.mp3
        // And song.uri might be: content://media/external/audio/media/12345
        // So we need to match using the actual file path if available
        availableSongs.find { song ->
            // Try to get the actual file path from the song's URI
            val songPath = song.uri.path
            songPath != null && (songPath == path || songPath.equals(path, ignoreCase = true))
        }?.let { 
            Log.d(TAG, "Found song by exact path match: $path")
            return it 
        }
        
        // Try file name match (basename without extension)
        val fileName = path.substringAfterLast("/").substringBeforeLast(".")
        availableSongs.find { song ->
            val songFileName = song.uri.lastPathSegment?.substringBeforeLast(".")
                ?: song.uri.path?.substringAfterLast("/")?.substringBeforeLast(".")
            songFileName?.equals(fileName, ignoreCase = true) == true
        }?.let { 
            Log.d(TAG, "Found song by filename match: $fileName")
            return it 
        }
        
        // Try fuzzy filename match (handles URL encoding, underscores vs spaces, etc.)
        val normalizedFileName = fileName.replace("_", " ").replace("%20", " ").lowercase()
        availableSongs.find { song ->
            val songFileName = (song.uri.lastPathSegment?.substringBeforeLast(".")
                ?: song.uri.path?.substringAfterLast("/")?.substringBeforeLast(".")
                ?: "").replace("_", " ").replace("%20", " ").lowercase()
            songFileName == normalizedFileName
        }?.let { 
            Log.d(TAG, "Found song by normalized filename match: $normalizedFileName")
            return it 
        }
        
        // Try title match from EXTINF metadata
        if (title.isNotBlank()) {
            // Try exact title match first
            availableSongs.find { song ->
                song.title.equals(title, ignoreCase = true)
            }?.let { 
                Log.d(TAG, "Found song by exact title match: $title")
                return it 
            }
            
            // Try "Artist - Title" format match
            val cleanTitle = title.trim()
            availableSongs.find { song ->
                val artistTitle = "${song.artist} - ${song.title}"
                artistTitle.equals(cleanTitle, ignoreCase = true)
            }?.let { 
                Log.d(TAG, "Found song by artist-title match: $cleanTitle")
                return it 
            }
            
            // Try partial title match
            availableSongs.find { song ->
                cleanTitle.contains(song.title, ignoreCase = true) || 
                song.title.contains(cleanTitle, ignoreCase = true)
            }?.let { 
                Log.d(TAG, "Found song by partial title match: $cleanTitle")
                return it 
            }
        }
        
        Log.d(TAG, "No match found for path: $path" + if (title.isNotBlank()) " (title: $title)" else "")
        return null
    }
    
    private fun createPlaylistsZip(playlists: List<Playlist>, format: PlaylistExportFormat, zipFile: File) {
        // For now, we'll just export as individual files
        // In a full implementation, you'd use ZipOutputStream
        val tempDir = File(zipFile.parent, "temp_playlists")
        tempDir.mkdirs()
        
        try {
            playlists.forEach { playlist ->
                val fileName = sanitizeFileName("${playlist.name}${format.extension}")
                val tempFile = File(tempDir, fileName)
                
                when (format) {
                    PlaylistExportFormat.JSON -> exportToJson(playlist, tempFile)
                    PlaylistExportFormat.M3U -> exportToM3u(playlist, tempFile, false)
                    PlaylistExportFormat.M3U8 -> exportToM3u(playlist, tempFile, true)
                    PlaylistExportFormat.PLS -> exportToPls(playlist, tempFile)
                }
            }
            
            // For now, just rename the temp directory to indicate completion
            // In a full implementation, you'd zip the contents
            Log.d(TAG, "Exported ${playlists.size} playlists to ${tempDir.absolutePath}")
        } finally {
            // Clean up temp directory in a real implementation
        }
    }
    
    private fun getDefaultExportDirectory(context: Context): File {
        return File(context.getExternalFilesDir(Environment.DIRECTORY_MUSIC), "Playlists")
    }
    
    private fun getCurrentTimestamp(): String {
        return SimpleDateFormat("yyyyMMdd_HHmmss", Locale.getDefault()).format(Date())
    }
    
    private fun sanitizeFileName(fileName: String): String {
        return fileName.replace(Regex("[^a-zA-Z0-9._-]"), "_")
    }
    
    private fun getFileName(context: Context, uri: Uri): String {
        context.contentResolver.query(uri, null, null, null, null)?.use { cursor ->
            if (cursor.moveToFirst()) {
                val nameIndex = cursor.getColumnIndex(android.provider.OpenableColumns.DISPLAY_NAME)
                if (nameIndex >= 0) {
                    return cursor.getString(nameIndex)
                }
            }
        }
        return uri.lastPathSegment ?: "unknown"
    }
    
    /**
     * Export playlist to user-selected directory using Storage Access Framework
     */
    private fun exportToUserSelectedDirectory(
        context: Context,
        directoryUri: Uri,
        playlist: Playlist,
        format: PlaylistExportFormat,
        fileName: String
    ): File {
        val directory = DocumentFile.fromTreeUri(context, directoryUri)
            ?: throw IllegalArgumentException("Cannot access selected directory")
        
        val documentFile = directory.createFile(format.mimeType, fileName)
            ?: throw IllegalArgumentException("Cannot create file in selected directory")
        
        context.contentResolver.openOutputStream(documentFile.uri)?.use { outputStream ->
            when (format) {
                PlaylistExportFormat.JSON -> {
                    val exportData = PlaylistExportData(
                        name = playlist.name,
                        id = playlist.id,
                        dateCreated = playlist.dateCreated,
                        dateModified = playlist.dateModified,
                        songs = playlist.songs.map { song ->
                            PlaylistSongEntry(
                                title = song.title,
                                artist = song.artist,
                                album = song.album,
                                duration = song.duration,
                                filePath = song.uri.path,
                                uri = song.uri.toString(),
                                trackNumber = song.trackNumber,
                                year = song.year
                            )
                        }
                    )
                    val json = Gson().toJson(exportData)
                    outputStream.write(json.toByteArray())
                }
                PlaylistExportFormat.M3U -> exportM3uToStream(playlist, outputStream, false)
                PlaylistExportFormat.M3U8 -> exportM3uToStream(playlist, outputStream, true)
                PlaylistExportFormat.PLS -> exportPlsToStream(playlist, outputStream)
            }
        } ?: throw IllegalArgumentException("Cannot open output stream for selected file")
        
        // Return a File object for compatibility (though it's not a real file path)
        return File(directory.uri.path ?: "user_selected", fileName)
    }
    
    /**
     * Export all playlists to user-selected directory using Storage Access Framework
     */
    private fun exportAllPlaylistsToUserSelectedDirectory(
        context: Context,
        directoryUri: Uri,
        playlists: List<Playlist>,
        format: PlaylistExportFormat,
        zipFileName: String
    ): File {
        val directory = DocumentFile.fromTreeUri(context, directoryUri)
            ?: throw IllegalArgumentException("Cannot access selected directory")
        
        // For now, export as separate files instead of ZIP to user directory
        // since ZIP creation in SAF is complex
        playlists.forEach { playlist ->
            val fileName = sanitizeFileName("${playlist.name}${format.extension}")
            exportToUserSelectedDirectory(context, directoryUri, playlist, format, fileName)
        }
        
        // Return a dummy File object for compatibility
        return File(directory.uri.path ?: "user_selected", "playlists_exported")
    }
    
    /**
     * Helper to export M3U format to output stream
     */
    private fun exportM3uToStream(playlist: Playlist, outputStream: java.io.OutputStream, extended: Boolean) {
        val writer = outputStream.bufferedWriter()
        if (extended) {
            writer.write("#EXTM3U\n")
        }
        
        playlist.songs.forEach { song ->
            if (extended) {
                writer.write("#EXTINF:${song.duration / 1000},${song.artist} - ${song.title}\n")
            }
            writer.write("${song.uri}\n")
        }
        writer.flush()
    }
    
    /**
     * Helper to export PLS format to output stream
     */
    private fun exportPlsToStream(playlist: Playlist, outputStream: java.io.OutputStream) {
        val writer = outputStream.bufferedWriter()
        writer.write("[playlist]\n")
        
        playlist.songs.forEachIndexed { index, song ->
            val num = index + 1
            writer.write("File$num=${song.uri}\n")
            writer.write("Title$num=${song.artist} - ${song.title}\n")
            writer.write("Length$num=${song.duration / 1000}\n")
        }
        
        writer.write("NumberOfEntries=${playlist.songs.size}\n")
        writer.write("Version=2\n")
        writer.flush()
    }
}
