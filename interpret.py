import csv
from collections import Counter

# -------------------------------
# Parse fetch_latimes.csv for fetch stats
# -------------------------------
fetch_attempted = 0
fetch_statuses = Counter()

with open('fetch_latimes.csv', newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        fetch_attempted += 1
        status = row['Status'].strip()
        fetch_statuses[status] += 1

# Assuming "200" indicates success; note that sometimes it may be "200 OK" depending on your code.
fetch_succeeded = fetch_statuses.get("200", 0)
fetch_failed = fetch_attempted - fetch_succeeded

# -------------------------------
# Parse visit_latimes.csv for file sizes and content types
# -------------------------------
visit_file_sizes = Counter()
content_types = Counter()

# Define file size ranges in bytes
KB = 1024
MB = 1024 * KB

with open('visit_latimes.csv', newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        try:
            size = int(row['File Size (Bytes)'])
        except ValueError:
            continue  # skip if size is not an integer
        # Categorize file sizes
        if size < KB:
            visit_file_sizes["< 1KB"] += 1
        elif size < 10 * KB:
            visit_file_sizes["1KB ~ <10KB"] += 1
        elif size < 100 * KB:
            visit_file_sizes["10KB ~ <100KB"] += 1
        elif size < MB:
            visit_file_sizes["100KB ~ <1MB"] += 1
        else:
            visit_file_sizes[">= 1MB"] += 1

        # Process content types (strip out encoding details, e.g., ";charset=utf-8")
        ctype = row['Content Type'].split(";")[0].strip()
        content_types[ctype] += 1

# -------------------------------
# Parse urls_latimes.csv for outgoing URL stats
# -------------------------------
total_urls_extracted = 0
all_urls = set()
urls_within = set()
urls_outside = set()

with open('urls_latimes.csv', newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        total_urls_extracted += 1
        url = row['URL'].strip()
        all_urls.add(url)
        indicator = row['Indicator'].strip()
        if indicator == "OK":
            urls_within.add(url)
        elif indicator == "N_OK":
            urls_outside.add(url)

unique_urls_extracted = len(all_urls)
unique_urls_within = len(urls_within)
unique_urls_outside = len(urls_outside)

# -------------------------------
# Generate Report (CrawlReport_latimes.txt)
# -------------------------------
report_lines = []

report_lines.append("Name: Simon Lewis")
report_lines.append("USC ID: 6580742939")
report_lines.append("News site crawled: latimes.com")
# Adjust the thread count if necessary.
report_lines.append("Number of threads: 8")
report_lines.append("")

report_lines.append("Fetch Statistics")
report_lines.append("================")
report_lines.append(f"# fetches attempted: {fetch_attempted}")
report_lines.append(f"# fetches succeeded: {fetch_succeeded}")
report_lines.append(f"# fetches failed or aborted: {fetch_failed}")
report_lines.append("")

report_lines.append("Outgoing URLs:")
report_lines.append("==============")
report_lines.append(f"Total URLs extracted: {total_urls_extracted}")
report_lines.append(f"# unique URLs extracted: {unique_urls_extracted}")
report_lines.append(f"# unique URLs within News Site: {unique_urls_within}")
report_lines.append(f"# unique URLs outside News Site: {unique_urls_outside}")
report_lines.append("")

report_lines.append("Status Codes:")
report_lines.append("=============")
# Output status codes for the ones that occurred. Adjust the order as needed.
for code in sorted(fetch_statuses.keys()):
    count = fetch_statuses[code]
    if count > 0:
        if code == "200":
            report_lines.append(f"200 OK: {count}")
        elif code == "301":
            report_lines.append(f"301 Moved Permanently: {count}")
        elif code == "401":
            report_lines.append(f"401 Unauthorized: {count}")
        elif code == "403":
            report_lines.append(f"403 Forbidden: {count}")
        elif code == "404":
            report_lines.append(f"404 Not Found: {count}")
        else:
            # For any additional status codes that occurred.
            report_lines.append(f"{code}: {count}")
report_lines.append("")

report_lines.append("File Sizes:")
report_lines.append("===========")
# Only output ranges that have nonzero counts.
for label in ["< 1KB", "1KB ~ <10KB", "10KB ~ <100KB", "100KB ~ <1MB", ">= 1MB"]:
    if label in visit_file_sizes:
        report_lines.append(f"{label}: {visit_file_sizes[label]}")
report_lines.append("")

report_lines.append("Content Types:")
report_lines.append("==============")
for ctype, count in content_types.items():
    report_lines.append(f"{ctype}: {count}")

# Write the report to a text file
with open("CrawlReport_latimes.txt", "w", encoding="utf-8") as report_file:
    for line in report_lines:
        report_file.write(line + "\n")

# Also print the report to the console
print("\n".join(report_lines))
