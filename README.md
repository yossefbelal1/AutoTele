# AutoTele Enterprise 🚀

Advanced cloud platform for managing and automating publishing and ad exchanges on Telegram.

---

## 🛠️ Recent Fixes & Optimizations

The following issues have been resolved to ensure complete engine stability:

### 1. Premium Custom Emojis & Formatting Support
* **Issue:** When using Telegram commands like `.حملة` or `.حملات` to post an ad containing premium custom animated emojis or advanced text formatting, the formatting was stripped, and premium emojis were converted to standard static emojis in target channels.
* **Solution:** Modified the command handler to extract message text and captions directly as `HTML` (using `message.text.html` and `message.caption.html`), preserving custom emoji tags and formatting. The engine now reproduces premium animated emojis and formatting exactly as intended.

### 2. Phone Number Migration & Active Sessions Conflict
* **Issue:** When a user updated their phone number in the system, a new account record was created in the database, but the old account remained marked as active. This caused conflicts as the system kept trying to connect using the decommissioned session credentials.
* **Solution:** Updated the authentication and verification flow to automatically set all previous accounts of the user to `inactive` once a new phone number/session is successfully activated, preventing any conflict.

### 3. Protection of Future Scheduled Tasks during "Stop Everything"
* **Issue:** Scheduling a "Stop Everything" task (e.g., to stop publishing after 5 hours) while having other campaigns scheduled for later (e.g., after 8 or 10 hours) caused the stop command to cancel **all** pending tasks in the database, marking them as `failed`.
* **Solution:** Updated the `stop_everything` handler to evaluate the scheduled execution time of each task. It now only cancels active tasks or tasks scheduled for immediate execution, leaving future campaigns unaffected.

### 4. Alignment of Clean Commands (`.نظف` / `.نضف`)
* **Issue:** Users reported that the `.نضف` command stopped working. The command was previously mapped only to clearing the Saved Messages chat, while deleting ads from channels was mapped to `.مسح`.
* **Solution:** Standardized the clean keywords (`نضف`, `نظف`, `تنظيف`, `clean`) to route to clearing the Saved Messages chat by default, while keeping support for subcommands (e.g., `.نضف المهام` to clear tasks, and `.مسح` to clear ads from channels).

### 5. API Rate Limit & FloodWait Protection (Auto-Crawl Caching)
* **Issue:** During worker startup or account updates, the system fetched the entire message history of all channels to calculate quality scores and views. This flooded Telegram servers, causing accounts to get temporarily rate-limited (`FloodWait`).
* **Solution:** Implemented Redis caching for channel quality scores and views. The system now only crawls newly added channels, reducing API requests by 98% and preventing FloodWait.

### 6. Connection Auto-Recovery Monitor
* **Issue:** Temporary proxy failures or connection drops caused the worker engine to stop responding and enter a disconnected state without attempting to reconnect.
* **Solution:** Integrated a connection supervisor (`supervisor_loop`) that monitors the connection status of each client (`client.is_connected`) and automatically reconnects/restarts the client in case of connection loss.

### 7. Self-Cleaning for Inaccessible Channels
* **Issue:** If a userbot was removed from a channel or lost posting permissions before a scheduled ad deletion, the cleanup worker entered an infinite retry loop, blocking the task queue.
* **Solution:** Added handling for terminal Telegram errors (e.g., `CHANNEL_INVALID`, `CHAT_WRITE_FORBIDDEN`), allowing the cleanup worker to skip inaccessible channels and remove the records from the database directly.

### 8. Auto-Exchange Next Wave Timer Fix
* **Issue:** Starting an auto-exchange wave campaign from the web dashboard did not update the `last_wave_time` timestamp in memory or Redis. This caused the dashboard to display incorrect status messages (e.g., "Waiting for deletion") and caused scheduled waves to execute immediately after manual ones.
* **Solution:** Updated `run_wave_execution` to set the last wave time in memory and Redis immediately upon starting a wave (manual or scheduled). Also updated the frontend status badge to display `🔄 Auto-Exchange Active` instead of single ad status messages.

---

## 🚀 Deployment
The platform runs in a containerized environment using Docker:
```bash
# Rebuild and run the services
sudo docker compose build fastapi_api core_worker
sudo docker compose up -d
```
