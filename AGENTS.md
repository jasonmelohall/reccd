# AGENTS.md

## Cursor Cloud specific instructions

### Overview

Reccd is a full-stack personalized Amazon product recommendation app:
- **Frontend**: React Native + Expo (web mode at `localhost:19006`)
- **Backend**: FastAPI + Python 3.11 (at `localhost:8000`)

### Prerequisites (system-level, already installed in snapshot)

- Python 3.11 via deadsnakes PPA (`sudo add-apt-repository -y ppa:deadsnakes/ppa && sudo apt-get install -y python3.11 python3.11-venv`)
- Node.js 22.x (pre-installed)

### Running the backend

```bash
cd backend
source venv/bin/activate
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

- The backend requires a `backend/.env` file with `DB_PASSWORD`, `RAINFOREST_API_KEY`, and `KEEPA_API_KEY`.
- For local dev without real credentials, set `DB_HOST=127.0.0.1` so the MySQL connection fails fast (refused) instead of hanging on the unreachable AWS RDS endpoint. The server still starts and serves all endpoints; the health check shows `"database": "disconnected"`.
- Interactive API docs: http://localhost:8000/docs

### Running the frontend

```bash
npx expo start --web --port 19006
```

- Placeholder assets are created by `node scripts/create-placeholder-assets.js` (run automatically during setup).
- The frontend reads `apiBaseUrl` from `app.json` → `expo.extra.apiBaseUrl`. By default it points to the production Render backend. To use the local backend, change `apiBaseUrl` to `http://localhost:8000` in `app.json`.

### Key gotchas

- The backend venv **must** use Python 3.11 (not the system 3.12). The venv is at `backend/venv/`.
- If the `backend/.env` points `DB_HOST` to the default AWS RDS host without valid credentials, the `uvicorn` startup will hang for 30+ seconds waiting for the TCP connection to time out. Always use `DB_HOST=127.0.0.1` for local dev without a real database.
- Frontend npm dependencies use `package-lock.json` — always use `npm install`, not yarn/pnpm.
- Expo may warn about package version mismatches; these are non-blocking warnings.
