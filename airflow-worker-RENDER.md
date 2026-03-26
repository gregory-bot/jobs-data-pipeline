# Instructions for Deploying Airflow Worker on Render

1. **Create a new Render service:**
   - Go to your Render dashboard and click "New +" > "Web Service" (for UI) or "Background Worker" (for scheduler only).
   - Connect to your GitHub repo.
   - Set the build path to `/airflow-worker.Dockerfile`.

2. **Set environment variables:**
   - Add variables from `airflow-worker.env.example` (copy and fill in real secrets).
   - Use the same DB as your FastAPI backend if you want shared data.

3. **Persistent storage (optional):**
   - For SQLite, no extra setup is needed (not recommended for production).
   - For Postgres/MySQL, set up a managed DB and update `AIRFLOW__CORE__SQL_ALCHEMY_CONN`.

4. **Start command:**
   - For scheduler: `airflow scheduler`
   - For webserver (UI): `airflow webserver`

5. **Deploy!**

---

**Note:**
- You can run both scheduler and webserver as separate Render services if you want the Airflow UI.
- For production, use a real database and secure your secrets.
