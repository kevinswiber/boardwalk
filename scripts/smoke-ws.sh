#!/usr/bin/env bash
#
# Smoke test: WS slow-consumer disconnect protocol.
#
# Usage:
#   1. In one terminal, start the demo hub:
#        cargo run --bin hello-led
#   2. In another terminal, run this script:
#        bash scripts/smoke-ws.sh
#
# What it does:
#   - Looks up the LED's device id at /servers/hub
#   - Opens a WS to /events and subscribes with outboundCapacity=1
#   - Reads the subscribe-ack, then *stops reading* for several seconds
#   - Bursts background transitions so the bus's bounded subscription
#     overflows under Lossless safety
#   - Resumes reading and prints the stream-gap that the WS forwarder
#     emits over its out-of-band terminal channel
#
# Requires: bash, curl, jq, python3 (stdlib only — no extra deps).

set -euo pipefail

HOST=${HOST:-http://localhost:1337}
WS_HOST=${WS_HOST:-localhost:1337}

if ! curl -sf -o /dev/null "$HOST/servers/hub"; then
    echo "error: $HOST/servers/hub did not respond" >&2
    echo "       run 'cargo run --bin hello-led' first" >&2
    exit 1
fi

ID=$(curl -sf "$HOST/servers/hub" | jq -r '.entities[0].properties.id')
TOPIC="hub/led/${ID}/state"

echo "device id: $ID"
echo "topic:     $TOPIC"
echo "ws host:   $WS_HOST"
echo

# Burst-fire background transitions so the bus subscription (cap 1)
# fills while the WS reader is paused inside Python. Parallel POSTs
# saturate localhost TCP buffers faster than a serial loop.
N_BURST=${N_BURST:-400}
(
    sleep 1.0
    for i in $(seq 1 "$N_BURST"); do
        if [[ $((i % 2)) -eq 0 ]]; then a=turn-off; else a=turn-on; fi
        curl -sf -X POST "$HOST/servers/hub/devices/$ID" \
             -H "content-type: application/x-www-form-urlencoded" \
             -d "action=$a" >/dev/null &
    done
    wait
) &
TRIGGER_PID=$!

python3 - "$WS_HOST" "$TOPIC" <<'PY'
"""
Minimal RFC 6455 client: opens the WS, sends a subscribe with
outboundCapacity=1, pauses without reading for several seconds so the
bus can overflow the bounded subscription, then drains frames and
prints the first stream-gap.

Uses only stdlib (socket / base64 / hashlib / os / json / struct).
"""
import base64, hashlib, json, os, socket, struct, sys, time

host_port, topic = sys.argv[1], sys.argv[2]
host, port = host_port.split(":")
port = int(port)

key = base64.b64encode(os.urandom(16)).decode()
req = (
    "GET /events HTTP/1.1\r\n"
    f"Host: {host_port}\r\n"
    "Upgrade: websocket\r\n"
    "Connection: Upgrade\r\n"
    f"Sec-WebSocket-Key: {key}\r\n"
    "Sec-WebSocket-Version: 13\r\n"
    "\r\n"
)
sock = socket.create_connection((host, port))
sock.sendall(req.encode())
buf = b""
while b"\r\n\r\n" not in buf:
    chunk = sock.recv(4096)
    if not chunk:
        sys.exit("error: WS handshake failed (server closed)")
    buf += chunk
if b" 101 " not in buf.split(b"\r\n", 1)[0]:
    sys.exit("error: WS handshake did not return 101: " + buf.split(b"\r\n", 1)[0].decode())

def send_text(payload):
    data = payload.encode()
    mask = os.urandom(4)
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
    header = bytes([0x81])
    n = len(data)
    if n < 126:
        header += bytes([0x80 | n])
    elif n < 65536:
        header += bytes([0x80 | 126]) + struct.pack(">H", n)
    else:
        header += bytes([0x80 | 127]) + struct.pack(">Q", n)
    sock.sendall(header + mask + masked)

def recv_exact(n):
    out = b""
    while len(out) < n:
        chunk = sock.recv(n - len(out))
        if not chunk:
            return None
        out += chunk
    return out

def recv_frame(timeout):
    sock.settimeout(timeout)
    try:
        head = recv_exact(2)
    except socket.timeout:
        return None
    if head is None:
        return None
    b1, b2 = head[0], head[1]
    fin = b1 & 0x80
    opcode = b1 & 0x0F
    masked = b2 & 0x80
    n = b2 & 0x7F
    if n == 126:
        n = struct.unpack(">H", recv_exact(2))[0]
    elif n == 127:
        n = struct.unpack(">Q", recv_exact(8))[0]
    mask = recv_exact(4) if masked else None
    payload = recv_exact(n) if n else b""
    if mask:
        payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    return opcode, payload

send_text(json.dumps({
    "type": "subscribe",
    "topic": topic,
    "outboundCapacity": 1,
}))

ack_op, ack_payload = recv_frame(2.0)
if ack_op != 0x1:
    sys.exit(f"error: expected text ack, got opcode {ack_op}")
ack = json.loads(ack_payload)
print("subscribe-ack:", json.dumps(ack, separators=(',', ': ')))
sub_id = ack.get("subscriptionId")

# Stop reading. The background curl bursts are firing transitions
# during this window; the bus's bounded subscription overflows under
# Lossless and arms slow_consumer_notify.
print("\npausing the reader for 5s so the bus subscription overflows...")
time.sleep(5.0)

print("draining frames, looking for stream-gap...\n")
deadline = time.time() + 10.0
events_seen = 0
gap = None
while time.time() < deadline:
    remaining = max(0.05, deadline - time.time())
    frame = recv_frame(remaining)
    if frame is None:
        break
    op, payload = frame
    if op == 0x8:
        print("(close frame received)")
        break
    if op != 0x1:
        continue
    msg = json.loads(payload)
    if msg.get("type") == "stream-gap":
        gap = msg
        break
    elif msg.get("type") == "event":
        events_seen += 1

print(f"events drained before gap: {events_seen}")
if gap is None:
    sys.exit("error: no stream-gap observed within 10s")

print("stream-gap:", json.dumps(gap, indent=2))
print()
print("contract check:")
print(f"  type:                  {gap.get('type')!r}  (expect 'stream-gap')")
print(f"  reason:                {gap.get('reason')!r}  (expect 'slow_consumer')")
print(f"  terminated:            {gap.get('terminated')!r}  (expect True)")
print(f"  subscriptionId:        {gap.get('subscriptionId')!r}  (expect {sub_id!r})")
print(f"  lastDeliveredSequence: {gap.get('lastDeliveredSequence')!r}  (expect >= 1)")
PY

wait "$TRIGGER_PID" 2>/dev/null || true
