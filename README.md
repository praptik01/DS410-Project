# Spotify Rec App – Developer Guide

This workspace has two parts that must run together:

1. **Model service (FastAPI + PySpark)** in the repo root. It loads `spotify_dataset.csv`, builds the playlist model, and exposes `POST /predict`.
2. **Next.js frontend** in `webapp/spotify-rec-app`. It lets users assemble playlists, hits `/api/predict`, and renders the results.

The sections below explain how to set everything up locally and how to troubleshoot the common issues we have already hit.

---

## Prerequisites

| Tool | Version/Notes |
| --- | --- |
| Python | 3.9+ (same as the PySpark environment) |
| Java | **OpenJDK 17**. PySpark 4.x will crash with Java 11 (class file version 55). |
| Node.js | 18+ (whatever `create-next-app` installed). |
| npm | 10+ (or yarn/pnpm if you prefer). |
| Dataset | `spotify_dataset.csv` must stay in the repo root so both services can read it. |

---

## 1. Python virtual environment & dependencies

From the repo root (`/Users/<you>/Desktop/DS410 Project`):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install fastapi uvicorn pyspark pandas
```

If you already installed these in your global interpreter you can skip the venv, but we recommend keeping everything isolated.

---

## 2. Java 17 configuration

PySpark 4.0 ships class files compiled for Java 17. Every time you run the FastAPI service you **must** export the Java 17 path in that shell:

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"
uvicorn app:app --host 127.0.0.1 --port 8000
```
Add those two lines to your `~/.zshrc` or shell profile so new terminals inherit them. Verify with `java -version`; you should see `openjdk version "17.x"`.

If you see `UnsupportedClassVersionError`, it means the shell running `uvicorn` is still using Java 11.
```

Notes:

* The first startup takes ~30–60 s while PySpark reads `spotify_dataset.csv`, builds proxy columns, and computes centroids. This is expected—wait until you see `Application startup complete.`.
* If you see `ModuleNotFoundError: pandas`, install it inside the same venv (`pip install pandas`). The service imports it even if we do not use it heavily.
* If `Predict` requests later return `ECONNREFUSED`, ensure this `uvicorn` process is still running.

---

## 4. Configure the frontend

In `webapp/spotify-rec-app/.env.local` (already committed) we set:

```
MODEL_URL=http://127.0.0.1:8000
```

If you run the model service on another host/port, update this file accordingly and restart `npm run dev`.

Install deps and start Next.js:

```bash
cd webapp/spotify-rec-app
npm install          # first time only
npm run dev
```

Open [http://localhost:3000](http://localhost:3000).

---

## 5. Using the app

1. Start typing a song title or artist—results come from the dataset (`/api/songs`). Each option carries a deterministic `track_id` so it lines up with the PySpark model.
2. Add at least one track to the playlist.
3. Click **Predict playlist**. The frontend POSTs to `/api/predict`, which proxies to FastAPI. The first prediction after a restart can take several seconds while PySpark warms caches.
4. The right panel shows:
   * `predicted_mood` + confidence (cosine similarity)
   * Top recommended tracks returned by the model (with mood scores)

---

## Troubleshooting

| Symptom | Likely Cause / Fix |
| --- | --- |
| `UnsupportedClassVersionError` when starting FastAPI | Java 11 is still active. Export `JAVA_HOME` to OpenJDK 17 in that shell. |
| `ModuleNotFoundError: pandas` | Install the dependency inside the same Python env the service uses. |
| `ECONNREFUSED 127.0.0.1:8000` in Next.js logs | Model service isn’t running or crashed. Restart `uvicorn app:app …`. |
| `JAVA_GATEWAY_EXITED ... SparkContext can only be used on the driver` | Happens if you import the module inside worker code. We now lazy-load Spark and guard everything with `_initialize_model()`, but if you see this again kill the existing Spark drivers and restart `uvicorn`. |
| Prediction button stays in “Generating…” forever | Check the FastAPI terminal for stack traces. Long pauses are normal right after startup but it should eventually log the `POST /predict` request. |
| `Missing expected column` | Ensure `spotify_dataset.csv` hasn’t been modified (column names must match the list in `spotify_playlist_generator.py`). |

---

## Common workflow for teammates

1. `git pull` to get the latest model/scripts.
2. `cd /path/to/DS410 Project && source .venv/bin/activate`.
3. Make sure Java 17 exports are in your shell (or run the `export` commands manually).
4. `uvicorn app:app --host 127.0.0.1 --port 8000` (leave running).
5. In another terminal: `cd webapp/spotify-rec-app && npm run dev`.
6. Use the UI normally. If anything fails, check `README.md` troubleshooting and the FastAPI logs first.

