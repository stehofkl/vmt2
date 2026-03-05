#!/bin/bash
# upload_csv.sh — Upload completed hourly CSV files to Node-RED HTTP endpoint
# Run via cron every hour, e.g.:
#   0 * * * * /home/pi/vmt/upload_csv.sh >> /home/pi/vmt/upload_csv.log 2>&1

# =============================================================================
# Config
# =============================================================================
CSV_DIR="/home/pi/vib"
ENDPOINT="https://mynodered.org/api/csv-upload"   # adjust to your Node-RED URL
LOG_PREFIX="[upload_csv]"

# =============================================================================
# Current hour file — skip this one as vib.py is still writing to it
# =============================================================================
CURRENT_FILE="${CSV_DIR}/$(date -u +%Y-%m-%d_%H).csv"

# =============================================================================
# Main
# =============================================================================
echo "$(date -u +"%Y-%m-%d %H:%M:%S") ${LOG_PREFIX} Starting upload run"

UPLOADED=0
FAILED=0

for FILE in "${CSV_DIR}"/*.csv; do
    # Skip if no files match glob
    [ -e "$FILE" ] || continue

    # Skip the file currently being written
    if [ "$FILE" = "$CURRENT_FILE" ]; then
        echo "$(date -u +"%Y-%m-%d %H:%M:%S") ${LOG_PREFIX} Skipping current: $(basename "$FILE")"
        continue
    fi

    echo "$(date -u +"%Y-%m-%d %H:%M:%S") ${LOG_PREFIX} Uploading: $(basename "$FILE")"

    HTTP_CODE=$(curl --silent --output /dev/null --write-out "%{http_code}" \
        --max-time 30 \
        --retry 3 \
        --retry-delay 5 \
        -X POST "${ENDPOINT}" \
        -H "Content-Type: text/csv" \
        -H "X-Filename: $(basename "$FILE")" \
        --data-binary "@${FILE}")

    if [ "$HTTP_CODE" -ge 200 ] && [ "$HTTP_CODE" -lt 300 ]; then
        echo "$(date -u +"%Y-%m-%d %H:%M:%S") ${LOG_PREFIX} OK (HTTP ${HTTP_CODE}): $(basename "$FILE") — deleting"
        rm "$FILE"
        UPLOADED=$((UPLOADED + 1))
    else
        echo "$(date -u +"%Y-%m-%d %H:%M:%S") ${LOG_PREFIX} FAILED (HTTP ${HTTP_CODE}): $(basename "$FILE") — keeping for retry"
        FAILED=$((FAILED + 1))
    fi
done

echo "$(date -u +"%Y-%m-%d %H:%M:%S") ${LOG_PREFIX} Done — uploaded: ${UPLOADED}, failed: ${FAILED}"
