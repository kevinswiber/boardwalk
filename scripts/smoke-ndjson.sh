#!/usr/bin/env bash
#
# Smoke test: HTTP NDJSON event stream carries envelope fields.
#
# Usage:
#   1. In one terminal, start the demo hub:
#        cargo run --bin hello-led
#   2. In another terminal, run this script:
#        bash scripts/smoke-ndjson.sh
#
# What it does:
#   - Looks up the LED's device id at /servers/hub
#   - Opens a streaming GET against /servers/hub/events?topic=hub/led/<id>/state
#   - In the background, POSTs `turn-on` so an envelope flows
#   - Prints the first NDJSON line and the envelope fields it carries

set -euo pipefail

HOST=${HOST:-http://localhost:1337}

if ! curl -sf -o /dev/null "$HOST/servers/hub"; then
    echo "error: $HOST/servers/hub did not respond" >&2
    echo "       run 'cargo run --bin hello-led' first" >&2
    exit 1
fi

ID=$(curl -sf "$HOST/servers/hub" | jq -r '.entities[0].properties.id')
TOPIC="hub/led/${ID}/state"
URL="$HOST/servers/hub/events?topic=$TOPIC"

echo "device id: $ID"
echo "topic:     $TOPIC"
echo "url:       $URL"
echo
echo "opening NDJSON stream + triggering one transition..."

# Open the NDJSON stream and capture the first line. Body is unbuffered
# so the line shows up the instant the server flushes it.
(
    sleep 0.15
    curl -sf -X POST "$HOST/servers/hub/devices/$ID" \
         -H "content-type: application/x-www-form-urlencoded" \
         -d "action=turn-on" >/dev/null
) &
TRIGGER_PID=$!

LINE=$(curl -Nsf --max-time 3 "$URL" | head -1 || true)
wait "$TRIGGER_PID" 2>/dev/null || true

if [[ -z "$LINE" ]]; then
    echo "error: no NDJSON line received within 3s" >&2
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
echo "legacy fields (byte-identical to pre-envelope wire):"
jq -r '"  topic:          " + (.topic          // "(missing)")' <<<"$LINE"
jq -r '"  timestamp:      " + ((.timestamp     // "(missing)") | tostring)' <<<"$LINE"
jq -r '"  data:           " + ((.data          // "(missing)") | tostring)' <<<"$LINE"
