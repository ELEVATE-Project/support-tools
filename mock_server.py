from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import json

app = FastAPI(title="Diksha Mock Server")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Load Postman collection
with open("Diksha.postman_collection.json") as f:
    collection = json.load(f)

MOCK_ROUTES = {}

def extract_items(items):
    for item in items:
        if "item" in item:
            extract_items(item["item"])
        else:
            method = item["request"]["method"]
            raw_url = item["request"]["url"]

            # Clean {{url}}, double slashes, and query strings
            if isinstance(raw_url, str):
                path = raw_url.replace("{{url}}", "").replace("//", "/")
                path = path.split("?")[0]  # strip query string from key
            else:
                # Filter empty segments to avoid double-slash (e.g. ["", "private", ...])
                segments = [p for p in raw_url.get("path", []) if p]
                path = "/" + "/".join(segments)

            # Strip trailing slash for consistent key storage
            path = path.rstrip("/") or "/"

            # Get first example response
            responses = item.get("response", [])
            if responses:
                example = responses[0]
                status_code = example.get("code", 200)
                body = example.get("body", "{}")

                try:
                    body_json = json.loads(body)
                except:
                    body_json = {"raw": body}

                MOCK_ROUTES[(method.upper(), path)] = (status_code, body_json)

extract_items(collection["item"])


def find_prefix_match(method: str, path: str, ignore_method: bool = False):
    """
    Fallback: find the stored route with the most matching path segments.
    Handles dynamic path values like UUIDs/IDs anywhere in the URL.
    e.g. /criteria/any-uuid  matches stored  /criteria
         /criteria/any-uuid  matches stored  /criteria/stored-uuid

    Set ignore_method=True to match regardless of HTTP method (last resort).
    """
    incoming_parts = [p for p in path.split("/") if p]
    best_match = None
    best_score = -1

    for (stored_method, stored_path), value in MOCK_ROUTES.items():
        if not ignore_method and stored_method != method:
            continue
        stored_parts = [p for p in stored_path.split("/") if p]

        # Count consecutive matching segments from the start
        score = 0
        for a, b in zip(incoming_parts, stored_parts):
            if a == b:
                score += 1
            else:
                break

        # Accept if all stored segments matched (incoming path has extra dynamic segments)
        # OR all-but-last stored segments matched (last stored segment is a different ID)
        min_required = max(1, len(stored_parts) - 1)
        if score >= min_required and score > best_score:
            best_score = score
            best_match = value

    return best_match


# Debug endpoint — list all loaded mock routes
@app.get("/_routes")
async def list_routes():
    return {
        "total": len(MOCK_ROUTES),
        "routes": [
            {"method": m, "path": p}
            for (m, p) in sorted(MOCK_ROUTES.keys())
        ]
    }


@app.api_route("/{full_path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def mock_all(request: Request, full_path: str):
    # Use request.url.path (strips query string), normalize slashes, strip trailing slash
    raw_path = request.url.path.replace("//", "/").rstrip("/") or "/"
    method = request.method.upper()

    # 1. Exact match
    key = (method, raw_path)
    if key in MOCK_ROUTES:
        status_code, response_body = MOCK_ROUTES[key]
        return JSONResponse(status_code=status_code, content=response_body)

    # 2. Prefix match — same method (handles dynamic segments like UUIDs)
    match = find_prefix_match(method, raw_path, ignore_method=False)
    if match:
        status_code, response_body = match
        return JSONResponse(status_code=status_code, content=response_body)

    # 3. Last resort — prefix match ignoring HTTP method
    #    (e.g. GET request for a route stored as POST in Postman)
    match = find_prefix_match(method, raw_path, ignore_method=True)
    if match:
        status_code, response_body = match
        return JSONResponse(status_code=status_code, content=response_body)

    return JSONResponse(
        status_code=404,
        content={"error": f"Mock not defined for {method} {raw_path}"}
    )