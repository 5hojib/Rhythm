package chromahub.rhythm.app.util

import android.app.Activity
import android.app.Application
import android.content.ClipData
import android.content.ClipboardManager
import android.content.Context
import android.content.Intent
import android.os.Bundle
import android.util.Log
import androidx.compose.foundation.layout.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.unit.dp
import androidx.compose.ui.window.Dialog
import androidx.compose.ui.window.DialogProperties
import chromahub.rhythm.app.data.AppSettings // Import AppSettings
import chromahub.rhythm.app.ui.screens.CrashActivity // Import CrashActivity
import kotlin.system.exitProcess

object CrashReporter {

    private const val TAG = "CrashReporter"
    private lateinit var appSettings: AppSettings // Declare AppSettings

    fun init(application: Application) {
        appSettings = AppSettings.getInstance(application) // Initialize AppSettings
        val defaultHandler = Thread.getDefaultUncaughtExceptionHandler()
        Thread.setDefaultUncaughtExceptionHandler { thread, throwable ->
            val crashLog = Log.getStackTraceString(throwable)
            Log.e(TAG, "Uncaught exception on thread ${thread.name}: $crashLog")
            appSettings.addCrashLogEntry(crashLog) // Add to crash log history
            CrashActivity.start(application.applicationContext, crashLog) // Start CrashActivity
            // Terminate the crashed process to ensure a clean restart
            android.os.Process.killProcess(android.os.Process.myPid())
            System.exit(1)
        }
    }

    // This function is no longer needed as CrashActivity handles the dialog
    // fun showDialog(log: String) {
    //     crashDetails = log
    //     showCrashDialog = true
    // }

    // This composable is no longer needed as CrashActivity hosts the dialog
    // @Composable
    // fun CrashDialog() {
    //     if (showCrashDialog) {
    //         val context = LocalContext.current
    //         Dialog(
    //             onDismissRequest = { /* Cannot dismiss */ },
    //             properties = DialogProperties(
    //                 dismissOnBackPress = false,
    //                 dismissOnClickOutside = false
    //             )
    //         ) {
    //             Surface(
    //                 shape = MaterialTheme.shapes.medium,
    //                 color = MaterialTheme.colorScheme.surface,
    //                 modifier = Modifier.fillMaxWidth()
    //             ) {
    //                 Column(
    //                     modifier = Modifier.padding(16.dp)
    //                 ) {
    //                     Text(
    //                         text = "Oops! Rhythm has crashed!",
    //                         style = MaterialTheme.typography.headlineSmall,
    //                         color = MaterialTheme.colorScheme.error
    //                     )
    //                     Spacer(modifier = Modifier.height(8.dp))
    //                     Text(
    //                         text = "An unexpected error occurred. Please copy the crash logs and report this issue. You can also restart the app.",
    //                         style = MaterialTheme.typography.bodyMedium
    //                     )
    //                     Spacer(modifier = Modifier.height(16.dp))
    //                     OutlinedTextField(
    //                         value = crashDetails ?: "No crash details available.",
    //                         onValueChange = { /* Read-only */ },
    //                         label = { Text("Crash Logs") },
    //                         readOnly = true,
    //                         modifier = Modifier
    //                             .fillMaxWidth()
    //                             .heightIn(max = 200.dp)
    //                     )
    //                     Spacer(modifier = Modifier.height(16.dp))
    //                     Row(
    //                         modifier = Modifier.fillMaxWidth(),
    //                         horizontalArrangement = Arrangement.End
    //                     ) {
    //                         Button(
    //                             onClick = {
    //                                 val intent = context.packageManager.getLaunchIntentForPackage(context.packageName)
    //                                 intent?.addFlags(Intent.FLAG_ACTIVITY_CLEAR_TOP or Intent.FLAG_ACTIVITY_NEW_TASK)
    //                                 context.startActivity(intent)
    //                                 exitProcess(0)
    //                             }
    //                         ) {
    //                             Text("Restart App")
    //                         }
    //                         Spacer(modifier = Modifier.width(8.dp))
    //                         Button(
    //                             onClick = {
    //                                 val clipboard = context.getSystemService(Context.CLIPBOARD_SERVICE) as ClipboardManager
    //                                 val clip = ClipData.newPlainText("Rhythm Crash Log", crashDetails)
    //                                 clipboard.setPrimaryClip(clip)
    //                                 exitProcess(0)
    //                             }
    //                         ) {
    //                             Text("Copy Logs & Close")
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //     }
    // }
}
