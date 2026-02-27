# Diksha Mock Server

A lightweight **FastAPI-based mock server** that automatically reads a Postman collection and serves mock API responses. Useful for local development and testing without hitting real backends.

---

## 📁 Project Structure

```
mockServer/
├── mock_server.py                  # Main FastAPI app
├── Diksha.postman_collection.json  # Postman collection (source of mock routes)
└── README.md                       # This file
```

---

## ⚙️ Prerequisites

- Python **3.8+**
- pip

---

## 🚀 Setup & Installation

### 1. Clone / Navigate to the project

```bash
cd /home/thippeswamy/Documents/mockServer
```

### 2. (Optional) Create a virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install fastapi uvicorn
```

---

## ▶️ Running the Mock Server

```bash
uvicorn mock_server:app --host 0.0.0.0 --port 8000
```

The server will start at: **http://localhost:8000**

> If port 8000 is already in use, find and kill the process:
> ```bash
> sudo lsof -i :8000        # find PID
> kill -9 <PID>             # kill it
> ```
> Or use a different port:
> ```bash
> uvicorn mock_server:app --host 0.0.0.0 --port 8800
> ```

### Auto-reload during development

```bash
uvicorn mock_server:app --host 0.0.0.0 --port 8000 --reload
```

---

## 🔄 How It Works

1. On startup, `mock_server.py` **reads** `Diksha.postman_collection.json`
2. It **extracts** every request + saved example response
3. It **registers** each as a route key: `(HTTP_METHOD, /path/without/query)`
4. Any incoming request is matched against this map and the saved response is returned
5. If no match is found → returns `404 {"error": "Mock not defined for METHOD /path"}`

### URL Normalization Rules

| Raw Postman URL | Stored Route Key |
|---|---|
| `{{url}}//api/data/v1/location/search` | `/api/data/v1/location/search` |
| `{{url}}/private/mlcore/api/v1/solutions/list?type=all` | `/private/mlcore/api/v1/solutions/list` |
| Dict `path: ["", "private", "mlcore"]` | `/private/mlcore` |

- `{{url}}` placeholder is stripped
- Double slashes `//` are normalized to `/`
- Query strings `?key=value` are **ignored** when matching (match on path only)

---

## 🌐 Exposing Locally via ngrok

**ngrok** creates a public HTTPS tunnel to your local server — useful for sharing with teammates or mobile testing.

### Step 1 — Install ngrok

**Option A: Download binary**
```bash
# Download
wget https://bin.equinox.io/c/bNyj1mQVY4c/ngrok-v3-stable-linux-amd64.tgz

# Extract
tar -xzf ngrok-v3-stable-linux-amd64.tgz

# Move to PATH
sudo mv ngrok /usr/local/bin/
```

**Option B: via snap (Ubuntu)**
```bash
sudo snap install ngrok
```

**Option C: via apt (if repo added)**
```bash
curl -s https://ngrok-agent.s3.amazonaws.com/ngrok.asc | sudo tee /etc/apt/trusted.gpg.d/ngrok.asc >/dev/null
echo "deb https://ngrok-agent.s3.amazonaws.com buster main" | sudo tee /etc/apt/sources.list.d/ngrok.list
sudo apt update && sudo apt install ngrok
```

### Step 2 — Sign up & get auth token

1. Go to [https://dashboard.ngrok.com/signup](https://dashboard.ngrok.com/signup)
2. Create a free account
3. Copy your **Authtoken** from the dashboard

### Step 3 — Authenticate ngrok

```bash
ngrok config add-authtoken <YOUR_AUTHTOKEN>
```

### Step 4 — Start the mock server

```bash
uvicorn mock_server:app --host 0.0.0.0 --port 8000
```

### Step 5 — Start ngrok tunnel (in a new terminal)

```bash
ngrok http 8000
```

You'll see output like:

```
Forwarding   https://abc123.ngrok-free.app -> http://localhost:8000
```

Use `https://abc123.ngrok-free.app` as the base URL for all API calls.

> ⚠️ Free ngrok URLs change every time you restart ngrok. Use a paid plan for a fixed domain.

---

## 🧪 Testing the Mock Server

### Using curl

```bash
# GET request
curl http://localhost:8000/api/data/v1/location/search

# POST request
curl -X POST http://localhost:8000/api/data/v1/location/search \
  -H "Content-Type: application/json" \
  -d '{}'
```

### Using Postman

Set your environment variable `{{url}}` to:
- Local: `http://localhost:8000`
- ngrok: `https://abc123.ngrok-free.app`

---

## 🛠️ Updating Mock Responses

1. Open your Postman collection
2. Update the **saved example response** for any request
3. Re-export the collection as `Diksha.postman_collection.json` into this folder
4. Restart the server:
   ```bash
   uvicorn mock_server:app --host 0.0.0.0 --port 8000
   ```

---

## ❗ Troubleshooting

| Problem | Cause | Fix |
|---|---|---|
| `404 Mock not defined` | Route not in Postman collection or no saved example | Add example response in Postman and re-export |
| `Address already in use` | Port 8000 taken | Kill existing process or use another port |
| All APIs return 404 | Wrong Postman JSON file name | Ensure file is named `Diksha.postman_collection.json` |
| ngrok URL not working | Server not running | Start uvicorn first, then ngrok |
| Query params not matching | Old version of code stored `?key=val` in route key | Update to latest `mock_server.py` |

---

## 📌 Quick Reference

```bash
# Start server
uvicorn mock_server:app --host 0.0.0.0 --port 8000

# Start with auto-reload
uvicorn mock_server:app --host 0.0.0.0 --port 8000 --reload

# Expose via ngrok
ngrok http 8000

# Kill process on port 8000
kill -9 $(lsof -t -i:8000)
```
