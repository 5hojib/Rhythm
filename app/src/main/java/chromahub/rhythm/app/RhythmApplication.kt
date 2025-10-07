package chromahub.rhythm.app

import android.app.Application
import android.os.Build
import android.util.Log
import chromahub.rhythm.app.data.AppSettings
import chromahub.rhythm.app.util.ANRWatchdog
import chromahub.rhythm.app.util.CrashReporter

/**
 * Custom Application class for Rhythm Music Player.
 * Handles initialization of:
 * - AppSettings
 * - CrashReporter
 * - NetworkClient
 * - LeakCanary (debug builds)
 * - ANR Watchdog (debug builds)
 */
class RhythmApplication : Application() {
    
    companion object {
        private const val TAG = "RhythmApplication"
        
        // Static reference to the application instance
        // Using a static reference is safe for Application class
        lateinit var instance: RhythmApplication
            private set
    }
    
    private var anrWatchdog: ANRWatchdog? = null
    
    override fun onCreate() {
        super.onCreate()
        
        instance = this
        
        Log.d(TAG, "═══════════════════════════════════════════════════")
        Log.d(TAG, "RhythmApplication onCreate")
        Log.d(TAG, "Build Type: ${BuildConfig.BUILD_TYPE}")
        Log.d(TAG, "Version: ${BuildConfig.VERSION_NAME}")
        Log.d(TAG, "═══════════════════════════════════════════════════")
        
        // Initialize AppSettings early (singleton, uses application context)
        AppSettings.getInstance(applicationContext)
        Log.d(TAG, "✓ AppSettings initialized")
        
        // Initialize CrashReporter
        CrashReporter.init(this)
        Log.d(TAG, "✓ CrashReporter initialized")
        
        // Initialize NetworkClient with AppSettings
        chromahub.rhythm.app.network.NetworkClient.initialize(
            AppSettings.getInstance(applicationContext)
        )
        Log.d(TAG, "✓ NetworkClient initialized")
        
        // Configure LeakCanary for debug builds
        if (BuildConfig.DEBUG) {
            configureLeakCanary()
            startANRWatchdog()
        }
        
        Log.d(TAG, "RhythmApplication initialization complete")
    }
    
    /**
     * Configure LeakCanary for optimal memory leak detection
     */
    private fun configureLeakCanary() {
        try {
            // LeakCanary 2.x auto-configures itself, but we can customize if needed
            // The library automatically watches Activities, Fragments, ViewModels, etc.
            Log.d(TAG, "✓ LeakCanary configured (auto-init)")
            
            // Optional: Customize LeakCanary behavior
            // You can add custom configuration here if needed
            // Example: LeakCanary.config = LeakCanary.config.copy(dumpHeap = true)
            
        } catch (e: Exception) {
            Log.e(TAG, "Error configuring LeakCanary", e)
        }
    }
    
    /**
     * Start ANR watchdog to monitor UI thread responsiveness
     */
    private fun startANRWatchdog() {
        try {
            // Start with 5 second timeout (standard ANR threshold)
            anrWatchdog = ANRWatchdog(timeoutMs = 5000).apply {
                start()
            }
            Log.d(TAG, "✓ ANR Watchdog started")
        } catch (e: Exception) {
            Log.e(TAG, "Error starting ANR Watchdog", e)
        }
    }
    
    override fun onTerminate() {
        Log.d(TAG, "RhythmApplication onTerminate")
        
        // Stop ANR watchdog
        anrWatchdog?.stopWatching()
        anrWatchdog = null
        
        super.onTerminate()
    }
    
    override fun onLowMemory() {
        super.onLowMemory()
        Log.w(TAG, "═══════════════════════════════════════════════════")
        Log.w(TAG, "LOW MEMORY WARNING!")
        Log.w(TAG, "═══════════════════════════════════════════════════")
        
        // Notify app components to clear caches
        // This could trigger cleanup in repositories, caches, etc.
        // You can add a broadcast or event here to notify components
    }
    
    override fun onTrimMemory(level: Int) {
        super.onTrimMemory(level)
        
        val levelName = when (level) {
            TRIM_MEMORY_RUNNING_MODERATE -> "RUNNING_MODERATE"
            TRIM_MEMORY_RUNNING_LOW -> "RUNNING_LOW"
            TRIM_MEMORY_RUNNING_CRITICAL -> "RUNNING_CRITICAL"
            TRIM_MEMORY_UI_HIDDEN -> "UI_HIDDEN"
            TRIM_MEMORY_BACKGROUND -> "BACKGROUND"
            TRIM_MEMORY_MODERATE -> "MODERATE"
            TRIM_MEMORY_COMPLETE -> "COMPLETE"
            else -> "UNKNOWN($level)"
        }
        
        Log.w(TAG, "onTrimMemory: $levelName")
        
        // Perform cleanup based on memory pressure level
        when (level) {
            TRIM_MEMORY_RUNNING_CRITICAL,
            TRIM_MEMORY_COMPLETE -> {
                Log.w(TAG, "Critical memory pressure - performing aggressive cleanup")
                // Trigger aggressive cleanup
                // You can broadcast an event here for repositories to clear caches
            }
            TRIM_MEMORY_RUNNING_LOW,
            TRIM_MEMORY_MODERATE -> {
                Log.w(TAG, "Moderate memory pressure - performing standard cleanup")
                // Trigger standard cleanup
            }
        }
    }
}
