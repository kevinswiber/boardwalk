#!/usr/bin/env bash
#
# Smoke test: job-runner HTTP NDJSON event stream carries envelope fields.
#
# Usage:
#   1. In one terminal, start the long-running job-runner example:
#        cargo run -p boardwalk-job-runner-example
#   2. In another terminal, run this script:
#        bash scripts/smoke-ndjson.sh
#
# What it does:
#   - Confirms the job-runner node is reachable at /servers/runner
#   - POSTs `submit` to the queue actor through /resources
#   - Opens the returned progress NDJSON stream for that job
#   - Prints the first NDJSON line and the envelope fields it carries

set -euo pipefail

HOST=${HOST:-http://localhost:4000}
QUEUE_ID=${QUEUE_ID:-queue-default}
SERVER=${SERVER:-runner}

if ! curl -sf -o /dev/null "$HOST/servers/$SERVER"; then
    echo "error: $HOST/servers/$SERVER did not respond" >&2
    echo "       run 'cargo run -p boardwalk-job-runner-example' first" >&2
    exit 1
fi

SUBMIT_URL="$HOST/resources/$QUEUE_ID/transitions/submit"
SUBMITTED=$(
    curl -sf -X POST "$SUBMIT_URL" \
        -H "content-type: application/json" \
        -d '{"command":{"type":"success-after-ticks","ticks":4},"owner":"smoke","priority":1}'
)

JOB_ID=$(jq -r '.output.jobId // empty' <<<"$SUBMITTED")
JOB_HREF=$(jq -r '.output.href // empty' <<<"$SUBMITTED")
PROGRESS_HREF=$(jq -r '.output.streams.progress // empty' <<<"$SUBMITTED")

if [[ -z "$JOB_ID" || -z "$PROGRESS_HREF" ]]; then
    echo "error: submit response did not include jobId and progress stream href" >&2
    echo "$SUBMITTED" | jq . >&2
    exit 1
fi

case "$PROGRESS_HREF" in
    http://*|https://*) URL="$PROGRESS_HREF" ;;
    /*) URL="$HOST$PROGRESS_HREF" ;;
    *) URL="$HOST/$PROGRESS_HREF" ;;
esac

echo "queue:    $QUEUE_ID"
echo "job id:   $JOB_ID"
echo "job href: $JOB_HREF"
echo "url:      $URL"
echo
echo "opening NDJSON stream for the submitted job..."

LINE=$(curl -Nsf --max-time 5 "$URL" | head -1 || true)

if [[ -z "$LINE" ]]; then
    echo "error: no NDJSON line received within 5s" >&2
    exit 1
fi

echo
echo "raw line:"
echo "$LINE"
echo
echo "pretty:"
echo "$LINE" | jq .

echo
echo "envelope fields:"
jq -r '"  eventId:        " + (.eventId        // "(missing)")' <<<"$LINE"
jq -r '"  streamId:       " + (.streamId       // "(missing)")' <<<"$LINE"
jq -r '"  sequence:       " + ((.sequence      // "(missing)") | tostring)' <<<"$LINE"
jq -r '"  nodeId:         " + (.nodeId         // "(missing)")' <<<"$LINE"
jq -r '"  resourceKind:   " + (.resourceKind   // "(missing)")' <<<"$LINE"
jq -r '"  resourceId:     " + (.resourceId     // "(missing)")' <<<"$LINE"
jq -r '"  payloadKind:    " + (.payloadKind    // "(missing)")' <<<"$LINE"
jq -r '"  payloadVersion: " + ((.payloadVersion // "(missing)") | tostring)' <<<"$LINE"
jq -r '"  envelopeVersion:" + ((.envelopeVersion// "(missing)") | tostring)' <<<"$LINE"
jq -r '"  isoTimestamp:   " + (.isoTimestamp   // "(missing)")' <<<"$LINE"
echo
echo "stream payload fields:"
jq -r '"  topic:          " + (.topic          // "(missing)")' <<<"$LINE"
jq -r '"  timestamp:      " + ((.timestamp     // "(missing)") | tostring)' <<<"$LINE"
jq -r '"  data:           " + ((.data          // "(missing)") | tostring)' <<<"$LINE"
