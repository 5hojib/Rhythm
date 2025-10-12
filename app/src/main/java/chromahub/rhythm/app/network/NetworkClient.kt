package chromahub.rhythm.app.network

import android.util.Log
import chromahub.rhythm.app.BuildConfig
import okhttp3.OkHttpClient
import okhttp3.logging.HttpLoggingInterceptor
import retrofit2.Retrofit
import retrofit2.converter.gson.GsonConverterFactory
import java.util.concurrent.TimeUnit
import okhttp3.ConnectionPool
import okhttp3.Interceptor
import okhttp3.Response
import java.io.IOException
import java.net.SocketTimeoutException
import java.net.UnknownHostException
import kotlin.math.pow
import chromahub.rhythm.app.network.LRCLibApiService
import chromahub.rhythm.app.network.DeezerApiService
import chromahub.rhythm.app.network.SpotifyCanvasApiService
import chromahub.rhythm.app.network.SpotifySearchApiService
import chromahub.rhythm.app.network.YTMusicApiService

object NetworkClient {
    private const val TAG = "NetworkClient"
    
    private const val LRCLIB_BASE_URL = "https://lrclib.net/"
    private const val DEEZER_BASE_URL = "https://api.deezer.com/"
    private const val CANVAS_BASE_URL = "https://api.paxsenix.org/spotify/"
    private const val YTMUSIC_BASE_URL = "https://music.youtube.com/"
    private const val SPOTIFY_API_BASE_URL = "https://api.spotify.com/v1/"
    private const val APPLEMUSIC_BASE_URL = "https://paxsenix.alwaysdata.net/"
    
    // Connection timeouts
    private const val CONNECT_TIMEOUT = 30L
    private const val READ_TIMEOUT = 30L
    private const val WRITE_TIMEOUT = 30L
    private const val MAX_RETRIES = 3
    
    private val connectionPool = ConnectionPool(5, 30, TimeUnit.SECONDS)
    
    // Store reference to AppSettings for dynamic API key
    private var appSettings: chromahub.rhythm.app.data.AppSettings? = null
    
    fun initialize(appSettings: chromahub.rhythm.app.data.AppSettings) {
        this.appSettings = appSettings
    }
    
    private val loggingInterceptor = HttpLoggingInterceptor { message ->
        try {
            Log.d(TAG, message)
        } catch (e: Exception) {
            Log.w(TAG, "Error logging HTTP message: ${e.message}")
        }
    }.apply {
        level = if (BuildConfig.DEBUG) HttpLoggingInterceptor.Level.HEADERS else HttpLoggingInterceptor.Level.NONE
    }
    
    private val retryInterceptor = Interceptor { chain ->
        var currentRetry = 0
        var response: Response? = null
        var lastException: IOException? = null
        
        while (currentRetry < MAX_RETRIES) {
            try {
                Log.d(TAG, "Attempting request (attempt ${currentRetry + 1}/${MAX_RETRIES}): ${chain.request().url}")
                response = chain.proceed(chain.request())
                
                if (response.isSuccessful) {
                    Log.d(TAG, "Request successful: ${chain.request().url}")
                    return@Interceptor response
                } else {
                    val code = response.code
                    Log.w(TAG, "Request failed with code $code: ${chain.request().url}")
                    
                    // Don't retry on client errors (4xx) except for specific cases
                    if (code in 400..499 && code != 408 && code != 429) {
                        Log.d(TAG, "Client error $code, not retrying")
                        return@Interceptor response
                    }
                    
                    // Handle rate limiting with exponential backoff
                    if (code == 429) {
                        val retryAfter = response.header("Retry-After")?.toLongOrNull() ?: (currentRetry + 1).toLong()
                        val backoffDelay = minOf(retryAfter * 1000, 30000) // Max 30 seconds
                        Log.d(TAG, "Rate limited, retrying after ${backoffDelay}ms")
                        response.close()
                        Thread.sleep(backoffDelay)
                        currentRetry++
                        continue
                    }
                    
                    response.close()
                }
            } catch (e: IOException) {
                lastException = e
                Log.e(TAG, "Request error (attempt ${currentRetry + 1}): ${e.javaClass.simpleName} - ${e.message}")
                
                // Classify errors for appropriate retry logic
                val shouldRetry = when (e) {
                    is SocketTimeoutException -> true
                    is UnknownHostException -> true
                    is java.net.ConnectException -> true
                    is java.net.SocketException -> true
                    is javax.net.ssl.SSLException -> false // Don't retry SSL errors
                    is java.io.FileNotFoundException -> false // Don't retry 404-like errors
                    else -> currentRetry < 1 // Only retry once for unknown errors
                }
                
                if (!shouldRetry) {
                    Log.d(TAG, "Error type ${e.javaClass.simpleName} is not retryable")
                    throw e
                }
            }
            
            currentRetry++
            if (currentRetry < MAX_RETRIES) {
                val baseDelay = 1000L
                val backoffDelay = minOf(baseDelay * (2.0.pow(currentRetry.toDouble())).toLong(), 10000L)
                Log.d(TAG, "Retrying after ${backoffDelay}ms delay")
                Thread.sleep(backoffDelay)
            }
        }
        
        // Return the last response if we have one, otherwise throw the last exception
        response?.let { return@Interceptor it }
        throw lastException ?: IOException("Request failed after $MAX_RETRIES retries")
    }
    
    private fun deezerHeadersInterceptor() = Interceptor { chain ->
        try {
            val request = chain.request().newBuilder()
                .header("User-Agent", "RhythmApp/2.9 (Android)")
                .header("Accept", "application/json")
                .build()
            chain.proceed(request)
        } catch (e: Exception) {
            Log.e(TAG, "Error in deezer headers interceptor: ${e.message}")
            throw e
        }
    }
    
    private val deezerHttpClient = OkHttpClient.Builder()
        .addInterceptor(deezerHeadersInterceptor())
        .addInterceptor(loggingInterceptor)
        .addInterceptor(retryInterceptor)
        .connectTimeout(CONNECT_TIMEOUT, TimeUnit.SECONDS)
        .readTimeout(READ_TIMEOUT, TimeUnit.SECONDS)
        .writeTimeout(WRITE_TIMEOUT, TimeUnit.SECONDS)
        .connectionPool(connectionPool)
        .build()
    
    private val deezerRetrofit = Retrofit.Builder()
        .baseUrl(DEEZER_BASE_URL)
        .client(deezerHttpClient)
        .addConverterFactory(GsonConverterFactory.create())
        .build()
    
    private val canvasHttpClient = OkHttpClient.Builder()
        .addInterceptor(deezerHeadersInterceptor())
        .addInterceptor(loggingInterceptor)
        .addInterceptor(retryInterceptor)
        .connectTimeout(CONNECT_TIMEOUT, TimeUnit.SECONDS)
        .readTimeout(READ_TIMEOUT, TimeUnit.SECONDS)
        .writeTimeout(WRITE_TIMEOUT, TimeUnit.SECONDS)
        .connectionPool(connectionPool)
        .build()
    
    private val canvasRetrofit = Retrofit.Builder()
        .baseUrl(CANVAS_BASE_URL)
        .client(canvasHttpClient)
        .addConverterFactory(GsonConverterFactory.create())
        .build()
    
    private val lrclibHttpClient = OkHttpClient.Builder()
        .addInterceptor(loggingInterceptor)
        .connectTimeout(CONNECT_TIMEOUT, TimeUnit.SECONDS)
        .readTimeout(READ_TIMEOUT, TimeUnit.SECONDS)
        .writeTimeout(WRITE_TIMEOUT, TimeUnit.SECONDS)
        .connectionPool(connectionPool)
        .build()
    
    private val lrclibRetrofit = Retrofit.Builder()
        .baseUrl(LRCLIB_BASE_URL)
        .client(lrclibHttpClient)
        .addConverterFactory(GsonConverterFactory.create())
        .build()
    
    private val ytmusicHttpClient = OkHttpClient.Builder()
        .addInterceptor(deezerHeadersInterceptor()) // same UA rules as Deezer
        .addInterceptor(loggingInterceptor)
        .addInterceptor(retryInterceptor)
        .connectTimeout(CONNECT_TIMEOUT, TimeUnit.SECONDS)
        .readTimeout(READ_TIMEOUT, TimeUnit.SECONDS)
        .writeTimeout(WRITE_TIMEOUT, TimeUnit.SECONDS)
        .connectionPool(connectionPool)
        .build()
    
    private val ytmusicRetrofit = Retrofit.Builder()
        .baseUrl(YTMUSIC_BASE_URL)
        .client(ytmusicHttpClient)
        .addConverterFactory(GsonConverterFactory.create())
        .build()
    
    private val spotifyHttpClient = OkHttpClient.Builder()
        .addInterceptor(loggingInterceptor)
        .addInterceptor(retryInterceptor)
        .connectTimeout(CONNECT_TIMEOUT, TimeUnit.SECONDS)
        .readTimeout(READ_TIMEOUT, TimeUnit.SECONDS)
        .writeTimeout(WRITE_TIMEOUT, TimeUnit.SECONDS)
        .connectionPool(connectionPool)
        .build()
    
    private val spotifyRetrofit = Retrofit.Builder()
        .baseUrl(SPOTIFY_API_BASE_URL)
        .client(spotifyHttpClient)
        .addConverterFactory(GsonConverterFactory.create())
        .build()
    
    private val appleMusicHttpClient = OkHttpClient.Builder()
        .addInterceptor(deezerHeadersInterceptor())
        .addInterceptor(loggingInterceptor)
        .addInterceptor(retryInterceptor)
        .connectTimeout(CONNECT_TIMEOUT, TimeUnit.SECONDS)
        .readTimeout(READ_TIMEOUT, TimeUnit.SECONDS)
        .writeTimeout(WRITE_TIMEOUT, TimeUnit.SECONDS)
        .connectionPool(connectionPool)
        .build()
    
    private val appleMusicRetrofit = Retrofit.Builder()
        .baseUrl(APPLEMUSIC_BASE_URL)
        .client(appleMusicHttpClient)
        .addConverterFactory(GsonConverterFactory.create())
        .build()
    
    val deezerApiService: DeezerApiService = deezerRetrofit.create(DeezerApiService::class.java)
    val canvasApiService: SpotifyCanvasApiService = canvasRetrofit.create(SpotifyCanvasApiService::class.java)
    val lrclibApiService: LRCLibApiService = lrclibRetrofit.create(LRCLibApiService::class.java)
    val ytmusicApiService: YTMusicApiService = ytmusicRetrofit.create(YTMusicApiService::class.java)
    val spotifySearchApiService: SpotifySearchApiService = spotifyRetrofit.create(SpotifySearchApiService::class.java)
    val appleMusicApiService: AppleMusicApiService = appleMusicRetrofit.create(AppleMusicApiService::class.java)
    
    // Generic OkHttp client for one-off requests (e.g., Wikidata JSON). Reuses header interceptor.
    val genericHttpClient: OkHttpClient = OkHttpClient.Builder()
        .addInterceptor(deezerHeadersInterceptor())
        .addInterceptor(loggingInterceptor)
        .build()
    
    // Helper methods to check if APIs are enabled
    fun isDeezerApiEnabled(): Boolean = appSettings?.deezerApiEnabled?.value ?: true
    fun isCanvasApiEnabled(): Boolean = appSettings?.canvasApiEnabled?.value ?: true
    fun isLrcLibApiEnabled(): Boolean = appSettings?.lrclibApiEnabled?.value ?: true
    fun isYTMusicApiEnabled(): Boolean = appSettings?.ytMusicApiEnabled?.value ?: true
    fun isSpotifyApiEnabled(): Boolean = appSettings?.spotifyApiEnabled?.value ?: false
    fun isAppleMusicApiEnabled(): Boolean = appSettings?.appleMusicApiEnabled?.value ?: true
    
    // Get Spotify API credentials
    fun getSpotifyClientId(): String = appSettings?.spotifyClientId?.value ?: ""
    fun getSpotifyClientSecret(): String = appSettings?.spotifyClientSecret?.value ?: ""
}