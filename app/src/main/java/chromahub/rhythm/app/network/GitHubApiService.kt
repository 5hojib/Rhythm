package chromahub.rhythm.app.network

import retrofit2.Response
import retrofit2.http.GET
import retrofit2.http.Headers
import retrofit2.http.Path
import retrofit2.http.Query

/**
 * GitHub Release model that matches the GitHub API release response format
 */
data class GitHubRelease(
    val id: Long,
    val tag_name: String,
    val name: String,
    val body: String,
    val draft: Boolean,
    val prerelease: Boolean,
    val published_at: String,
    val html_url: String,
    val assets: List<GitHubAsset>
)

/**
 * GitHub Asset model for release assets
 */
data class GitHubAsset(
    val id: Long,
    val name: String,
    val browser_download_url: String,
    val content_type: String,
    val size: Long,
    val state: String,
    val download_count: Long = 0
)

/**
 * Retrofit service interface for GitHub API
 */
interface GitHubApiService {
    /**
     * Fetch all releases for a repository
     * 
     * @param owner The GitHub username of the repository owner
     * @param repo The repository name
     * @param perPage Number of results per page (max 100)
     * @return List of releases
     */
    @GET("repos/{owner}/{repo}/releases")
    suspend fun getReleases(
        @Path("owner") owner: String,
        @Path("repo") repo: String,
        @Query("per_page") perPage: Int = 10
    ): Response<List<GitHubRelease>>
    
    /**
     * Fetch only the latest release
     * This excludes pre-releases and drafts
     * 
     * @param owner The GitHub username of the repository owner
     * @param repo The repository name
     * @return The latest release
     */
    @GET("repos/{owner}/{repo}/releases/latest")
    suspend fun getLatestRelease(
        @Path("owner") owner: String,
        @Path("repo") repo: String
    ): Response<GitHubRelease>
    
    /**
     * Fetch a specific release by its ID
     * 
     * @param owner The GitHub username of the repository owner
     * @param repo The repository name
     * @param releaseId The ID of the release
     * @return The requested release
     */
    @GET("repos/{owner}/{repo}/releases/{release_id}")
    suspend fun getRelease(
        @Path("owner") owner: String,
        @Path("repo") repo: String,
        @Path("release_id") releaseId: Long
    ): Response<GitHubRelease>
} 