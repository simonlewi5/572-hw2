import threading
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import csv
from queue import Queue
import logging
import pickle
import os

# ---------------- Configuration ----------------
root_url = "https://www.latimes.com"
max_pages = 20000
max_depth = 16
politeness_delay = 1         # 1 second delay between requests
num_threads = 8
STATE_FILE = "crawler_state.pkl"

# Allowed content types: HTML plus additional file types
allowed_types = {
    "application/pdf",
    "application/msword",
    "image/jpeg",
    "image/png",
    "image/gif"
}

# Custom headers to mimic a legitimate browser
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
}

# ---------------- Logging Setup ----------------
logging.basicConfig(
    filename="crawler.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logging.info("Crawler started.")

# ---------------- Shared Data Structures ----------------
fetch_data = []   # (URL, HTTP status)
visit_data = []   # (URL, file size, number of outlinks, content type)
urls_data = []    # (URL, indicator: "OK" for internal, "N_OK" for external)
visited = set()   # URLs already processed

# Locks for thread safety
visited_lock = threading.Lock()
data_lock = threading.Lock()

# Queue for URLs to visit; each element: (url, depth)
url_queue = Queue()

# ---------------- State Persistence Functions ----------------
def save_state():
    state = {
        "visited": visited,
        "queue": list(url_queue.queue),
        "fetch_data": fetch_data,
        "visit_data": visit_data,
        "urls_data": urls_data
    }
    with open(STATE_FILE, "wb") as f:
        pickle.dump(state, f)
    logging.info("State saved with %d visited pages.", len(visited))

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "rb") as f:
            state = pickle.load(f)
        logging.info("State loaded with %d visited pages.", len(state["visited"]))
        return state
    return None

# ---------------- Initialize or Resume State ----------------
state = load_state()
if state:
    visited = state["visited"]
    for item in state["queue"]:
        url_queue.put(item)
    fetch_data = state["fetch_data"]
    visit_data = state["visit_data"]
    urls_data = state["urls_data"]
else:
    url_queue.put((root_url, 0))  # Start fresh

# ---------------- Worker Function ----------------
def worker():
    while True:
        try:
            current_url, depth = url_queue.get(timeout=10)
        except Exception:
            break  # Exit if no new URL for a while

        with visited_lock:
            if current_url in visited or depth > max_depth or len(visited) >= max_pages:
                url_queue.task_done()
                continue
            visited.add(current_url)
        logging.info("Processing URL: %s at depth %d", current_url, depth)

        try:
            response = requests.get(current_url, headers=headers, timeout=10)
            status = response.status_code
        except Exception as e:
            with data_lock:
                fetch_data.append((current_url, "Error"))
            logging.error("Failed to fetch %s: %s", current_url, str(e))
            time.sleep(politeness_delay)
            url_queue.task_done()
            continue

        with data_lock:
            fetch_data.append((current_url, status))

        if status == 200:
            content_type_full = response.headers.get("Content-Type", "")
            content_type = content_type_full.split(";")[0].strip()
            file_size = len(response.content)
            num_outlinks = 0
            logging.info("Fetched %s with content type: %s and size: %d", current_url, content_type, file_size)
            
            if content_type in allowed_types or content_type.startswith("text/html"):
                if content_type.startswith("text/html"):
                    soup = BeautifulSoup(response.content, "html.parser")
                    outlinks = set()
                    for tag in soup.find_all("a", href=True):
                        href = tag["href"]
                        abs_url = urljoin(current_url, href)
                        outlinks.add(abs_url)
                        indicator = "OK" if urlparse(abs_url).netloc.endswith("latimes.com") else "N_OK"
                        with data_lock:
                            urls_data.append((abs_url, indicator))
                        if indicator == "OK":
                            with visited_lock:
                                if abs_url not in visited:
                                    url_queue.put((abs_url, depth + 1))
                    num_outlinks = len(outlinks)
                    logging.info("Extracted %d outlinks from %s", num_outlinks, current_url)
                    if num_outlinks == 0:
                        logging.warning("No outlinks found on %s", current_url)
                else:
                    num_outlinks = 0

                with data_lock:
                    visit_data.append((current_url, file_size, num_outlinks, content_type))
        else:
            logging.info("Non-200 status for %s: %d", current_url, status)

        with visited_lock:
            if len(visited) % 100 == 0:
                logging.info("Progress: %d pages processed.", len(visited))
                save_state()
        
        time.sleep(politeness_delay)
        url_queue.task_done()

# ---------------- Start Worker Threads ----------------
threads = []
for i in range(num_threads):
    t = threading.Thread(target=worker)
    t.start()
    threads.append(t)

url_queue.join()
for t in threads:
    t.join()

save_state()

# ---------------- Write Output CSV Files ----------------
with open("fetch_latimes.csv", "w", newline="", encoding="utf-8") as f_fetch:
    writer = csv.writer(f_fetch)
    writer.writerow(["URL", "Status"])
    writer.writerows(fetch_data)
logging.info("fetch_latimes.csv written with %d entries.", len(fetch_data))

with open("visit_latimes.csv", "w", newline="", encoding="utf-8") as f_visit:
    writer = csv.writer(f_visit)
    writer.writerow(["URL", "File Size (Bytes)", "Number of Outlinks", "Content Type"])
    writer.writerows(visit_data)
logging.info("visit_latimes.csv written with %d entries.", len(visit_data))

with open("urls_latimes.csv", "w", newline="", encoding="utf-8") as f_urls:
    writer = csv.writer(f_urls)
    writer.writerow(["URL", "Indicator"])
    writer.writerows(urls_data)
logging.info("urls_latimes.csv written with %d entries.", len(urls_data))

logging.info("Crawler finished. Total visited pages: %d", len(visited))
