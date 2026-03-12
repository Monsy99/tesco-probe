# multi-monitor

Background multi-endpoint probe service.  
Each URL gets its own independent async probe loop, circular buffer, and sparkline.

## Requirements

- **Node.js ≥ 18**
- One dependency: `express`

## Install & run

```bash
npm install
node server.js
```

Seed URLs at startup:

```bash
PROBE_URLS="https://api.example.com/health,https://other.com" node server.js
PORT=8080 node server.js
```

## Dashboard

`http://localhost:3000/` — served on demand.  
Auto-polls every 15 s. Pauses polling when the tab is hidden.

Each target card shows:

- Live status dot (green blink = up, red = down, yellow = probing)
- Availability %, last status, last latency, avg latency
- Latency sparkline
- Click card → full latency + availability charts + probe log

## REST API

| Method   | Path                     | Body / Notes                                              |
| -------- | ------------------------ | --------------------------------------------------------- |
| `GET`    | `/api/targets`           | List all targets with summaries                           |
| `POST`   | `/api/targets`           | `{ url, label?, intervalSec?, autoStart? }` — add & start |
| `DELETE` | `/api/targets/:id`       | Remove target (stops probe loop first)                    |
| `POST`   | `/api/targets/:id/start` | Start probe loop                                          |
| `POST`   | `/api/targets/:id/stop`  | Stop probe loop                                           |
| `GET`    | `/api/targets/:id/data`  | Full history `{ count, points[] }`                        |
| `DELETE` | `/api/targets/:id/data`  | Clear history                                             |
| `POST`   | `/api/targets/probe-all` | Fire immediate probe on all running targets               |

### Quick examples

```bash
# Add a target
curl -X POST http://localhost:3000/api/targets \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://httpbin.org/get","label":"httpbin","intervalSec":60}'

# List all
curl http://localhost:3000/api/targets | jq '.[].url'

# Get history for one target
curl http://localhost:3000/api/targets/<id>/data | jq '.count'

# Stop one
curl -X POST http://localhost:3000/api/targets/<id>/stop

# Remove one
curl -X DELETE http://localhost:3000/api/targets/<id>

# Trigger immediate probe on all running
curl -X POST http://localhost:3000/api/targets/probe-all
```

## Memory model

| Concern                    | Approach                                                                        |
| -------------------------- | ------------------------------------------------------------------------------- |
| Per-target circular buffer | Hard cap of **480 entries** (24 h @ 3 min). `Array.shift()` on overflow         |
| Overlapping probes         | `probing` boolean guard per target — skips if still in-flight                   |
| Socket leaks               | `res.resume()` drains response body, releasing the socket                       |
| Hanging requests           | 10 s timeout + `req.destroy()`                                                  |
| 24-hour runaway            | `setTimeout` auto-stop per target                                               |
| Target removal             | `stopTarget()` clears both `setInterval` and `setTimeout` before `Map.delete()` |
| Dashboard sparklines       | `Chart.destroy()` called when a card is removed from the DOM                    |
| Idle browser tab           | `visibilitychange` pauses the 15 s poll loop                                    |

## Run as a background service

### PM2

```bash
npm install -g pm2
pm2 start server.js --name multi-monitor
pm2 save && pm2 startup
```

### systemd

```ini
[Unit]
Description=Multi Monitor
After=network.target

[Service]
WorkingDirectory=/opt/multi-monitor
ExecStart=/usr/bin/node server.js
Restart=on-failure
Environment=PORT=3000
Environment=PROBE_URLS=https://api.example.com/health,https://other.com

[Install]
WantedBy=multi-user.target
```
