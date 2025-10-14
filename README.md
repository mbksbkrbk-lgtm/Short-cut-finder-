Gov Vacancy Finder - optimized (ready-to-deploy)

Files:
 - app.py  -> Flask optimized crawler and web UI endpoints
 - requirements.txt
 - Procfile

How to deploy (mobile-friendly):
1. Create a GitHub repo and upload these files (Add file -> Upload files).
2. On Render: New -> Web Service -> Connect GitHub -> select repo.
3. Build command: pip install -r requirements.txt
   Start command: gunicorn app:app
4. Deploy and open your public URL.
5. Visit /start-full-crawl to begin (background). Check /status and /results.
Note: Full crawl may take time; Render free limits may stop very long crawls. For full-scale, use a VPS.
