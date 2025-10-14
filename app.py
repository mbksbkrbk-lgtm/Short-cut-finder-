from flask import Flask, render_template_string, jsonify, send_file
import threading, time, csv, os, sqlite3
from datetime import datetime, date
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

app = Flask(__name__)

START_URL = "https://goidirectory.gov.in"
KEYWORDS = ["career","careers","recruit","recruitment","vacancy","vacancies","job","jobs","notification","advertisement","apply"]
MAX_ORGS = 200
REQUEST_SLEEP = 0.5
REQUEST_TIMEOUT = 10
DB = "gov_jobs.db"

def init_db():
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS jobs (id INTEGER PRIMARY KEY, source_site TEXT, dept_name TEXT, job_title TEXT, job_link TEXT, snippet TEXT, deadline_iso TEXT, deadline_unknown INTEGER, fetched_at TEXT)''')
    conn.commit(); conn.close()

init_db()

is_crawling = False
_last_request_time = {}

def polite_get(url):
    host = urlparse(url).netloc
    last = _last_request_time.get(host, 0)
    wait = REQUEST_SLEEP - (time.time() - last)
    if wait > 0:
        time.sleep(wait)
    try:
        r = requests.get(url, timeout=REQUEST_TIMEOUT, headers={'User-Agent':'Mozilla/5.0'})
        _last_request_time[host] = time.time()
        return r
    except:
        return None

def find_candidate_links(base_url, soup):
    anchors = soup.find_all("a", href=True)
    found = []
    for a in anchors:
        href = a['href'].strip(); text = (a.get_text() or "").strip()
        low = (href + " " + text).lower()
        if any(k in low for k in KEYWORDS):
            full = urljoin(base_url, href)
            found.append((text or full, full))
    seen = set(); uniq = []
    for t,u in found:
        if u not in seen:
            seen.add(u); uniq.append((t,u))
    return uniq

def extract_jobs_from_page(url):
    r = polite_get(url)
    if not r or r.status_code != 200: return []
    soup = BeautifulSoup(r.text, "html.parser")
    jobs = []
    blocks = soup.find_all(['article','li','tr','div','p'], limit=200)
    for b in blocks:
        text = b.get_text(" ", strip=True)
        if len(text) < 30: continue
        if any(k in text.lower() for k in ["last date","closing date","apply by","deadline","last date to apply","closing on"]):
            title = None
            for tag in ['h1','h2','h3','h4','strong','b']:
                t = b.find(tag)
                if t and t.get_text(strip=True):
                    title = t.get_text(strip=True); break
            if not title: title = text[:120]
            link_tag = b.find('a', href=True); link = url if not link_tag else urljoin(url, link_tag['href'])
            snippet = text[:400]
            deadline = None
            try:
                import re, dateparser
                m = re.search(r'(\d{1,2}[ /.-]\d{1,2}[ /.-]\d{2,4})', text)
                if m:
                    d = dateparser.parse(m.group(1))
                    if d: deadline = d.date().isoformat()
            except:
                pass
            jobs.append({'title': title, 'link': link, 'snippet': snippet, 'deadline': deadline, 'deadline_unknown': 0 if deadline else 1})
    for a in soup.find_all('a', href=True):
        href = a['href']
        if href.lower().endswith('.pdf') and any(k in (a.get_text() or href).lower() for k in ['notification','advertisement','recruit','vacancy','apply']):
            full = urljoin(url, href); title = a.get_text(strip=True) or full
            jobs.append({'title': title, 'link': full, 'snippet': 'PDF', 'deadline': None, 'deadline_unknown': 1})
    return jobs

def store_job(source_site, dept_name, job):
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute('INSERT INTO jobs (source_site, dept_name, job_title, job_link, snippet, deadline_iso, deadline_unknown, fetched_at) VALUES (?,?,?,?,?,?,?,datetime("now"))',
              (source_site, dept_name, job['title'], job['link'], job['snippet'], job['deadline'], job['deadline_unknown']))
    conn.commit(); conn.close()

def crawl_worker(orgs):
    saved = 0
    for name,url in orgs:
        try:
            r = polite_get(url); time.sleep(REQUEST_SLEEP)
            if not r or r.status_code != 200: continue
            soup = BeautifulSoup(r.text, "html.parser")
            candidates = find_candidate_links(url, soup)
            if not candidates:
                anchors = soup.find_all('a', href=True); subs = []
                for a in anchors[:60]:
                    full = urljoin(url, a['href'])
                    if urlparse(full).netloc == urlparse(url).netloc:
                        subs.append(full)
                for sub in subs[:12]:
                    r2 = polite_get(sub); time.sleep(REQUEST_SLEEP)
                    if not r2: continue
                    s2 = BeautifulSoup(r2.text, "html.parser")
                    found = find_candidate_links(sub, s2)
                    if found: candidates.extend(found)
            seen = set()
            for t,link in candidates[:6]:
                if link in seen: continue
                seen.add(link)
                jobs = extract_jobs_from_page(link)
                for job in jobs:
                    try:
                        if job['deadline'] and job['deadline'] < str(date.today()):
                            continue
                    except: pass
                    store_job(url, t or name, job); saved += 1
                time.sleep(REQUEST_SLEEP)
        except:
            continue
    return saved

def fetch_orgs(limit=MAX_ORGS):
    r = polite_get(START_URL)
    if not r or r.status_code != 200: return []
    soup = BeautifulSoup(r.text, "html.parser")
    anchors = soup.find_all('a', href=True)
    orgs = []
    for a in anchors:
        href = a['href']; text = (a.get_text() or "").strip()
        if not href: continue
        full = urljoin(START_URL, href)
        if ("gov.in" in full or "nic.in" in full or "govt.in" in full) and full not in [x[1] for x in orgs]:
            orgs.append((text or full, full))
        if len(orgs) >= limit: break
    return orgs

crawl_thread = None; _stop_flag = False

def full_crawl():
    orgs = fetch_orgs()
    batches = [orgs[i:i+8] for i in range(0, len(orgs), 8)]
    for batch in batches:
        if _stop_flag: break
        crawl_worker(batch)
    return True

@app.route('/')
def home():
    return "<h2>Gov Vacancy Finder (optimized)</h2><p>Use /start-full-crawl to begin. Check /status and /results.</p>"

@app.route('/start-full-crawl')
def start_full():
    global crawl_thread, _stop_flag
    if crawl_thread and crawl_thread.is_alive():
        return 'Crawl running', 400
    _stop_flag = False
    crawl_thread = threading.Thread(target=full_crawl, daemon=True)
    crawl_thread.start()
    return 'Crawl started', 200

@app.route('/status')
def status():
    running = crawl_thread.is_alive() if crawl_thread else False
    conn = sqlite3.connect(DB); c = conn.cursor(); c.execute('SELECT COUNT(*) FROM jobs'); total = c.fetchone()[0]; conn.close()
    return jsonify({'running': running, 'found': total})

@app.route('/results')
def results():
    conn = sqlite3.connect(DB); c = conn.cursor()
    c.execute('SELECT source_site, dept_name, job_title, job_link, snippet, deadline_iso, deadline_unknown, fetched_at FROM jobs ORDER BY fetched_at DESC LIMIT 1000')
    rows = c.fetchall(); conn.close()
    keys = ['source_site','dept_name','job_title','job_link','snippet','deadline_iso','deadline_unknown','fetched_at']
    out = [dict(zip(keys,row)) for row in rows]
    return jsonify(out)

@app.route('/download')
def download():
    conn = sqlite3.connect(DB); c = conn.cursor(); c.execute('SELECT source_site, dept_name, job_title, job_link, snippet, deadline_iso, deadline_unknown, fetched_at FROM jobs ORDER BY fetched_at DESC'); rows = c.fetchall(); conn.close()
    fname = f'gov_jobs_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    import csv as _csv
    with open(fname, 'w', newline='', encoding='utf-8') as f:
        writer = _csv.writer(f); writer.writerow(['source_site','dept_name','job_title','job_link','snippet','deadline_iso','deadline_unknown','fetched_at'])
        for r in rows: writer.writerow(r)
    return send_file(fname, as_attachment=True)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=10000)
