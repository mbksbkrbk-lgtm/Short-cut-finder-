# app.py
from flask import Flask, jsonify, render_template_string, send_file
import threading, time, csv, os
from datetime import datetime, date
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

app = Flask(__name__)

# ---------- CONFIG ----------
START_URL = "https://goidirectory.gov.in"
KEYWORDS = ["vacancy","vacancies","recruit","recruitment","career","careers","job","jobs","notification","advertisement","apply"]
MAX_ORGS = 300               # max orgs to consider from GoI directory
REQUEST_SLEEP = 0.5          # polite delay between requests to same host
REQUEST_TIMEOUT = 10
PER_ORG_CANDIDATES = 6      # how many candidate pages to visit per org
PER_ORG_SUBPAGES = 8        # how many internal sub-pages to scan if no direct candidate found

# skip-list: famous/public sites we DO NOT want to crawl (hosts/domains)
SKIP_HOSTS = [
    "upsc.gov.in",
    "ssc.nic.in",
    "indianrailways.gov.in",
    "railwayrecruitment.gov.in",
    "rbi.org.in",
    "nta.ac.in",
    "mhrd.gov.in",
    "indiacore.nic.in",
    "employmentnews.gov.in",
    "govtjobsportal.in",
    "ssc.gov.in",
    "joinindiancoastguard.gov.in",  # example
    "ncs.gov.in",
    "ntpc.co.in",
    "powergridindia.com",
    "psu.gov.in"
]

# storage (in-memory + optional csv files)
results = []
is_crawling = False
_last_request_time = {}

# ---------- helper functions ----------
def polite_get(url):
    host = urlparse(url).netloc
    last = _last_request_time.get(host, 0)
    wait = REQUEST_SLEEP - (time.time() - last)
    if wait > 0:
        time.sleep(wait)
    try:
        r = requests.get(url, timeout=REQUEST_TIMEOUT, headers={'User-Agent':'Mozilla/5.0 (compatible; GovVacBot/1.0)'})
        _last_request_time[host] = time.time()
        return r
    except Exception:
        return None

def is_skippable(url):
    host = urlparse(url).netloc.lower()
    for s in SKIP_HOSTS:
        if s in host:
            return True
    return False

def fetch_orgs(limit=MAX_ORGS):
    r = polite_get(START_URL)
    if not r or r.status_code != 200:
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    anchors = soup.find_all('a', href=True)
    orgs = []
    seen = set()
    for a in anchors:
        href = a['href'].strip()
        text = (a.get_text() or "").strip()
        if not href:
            continue
        full = urljoin(START_URL, href)
        parsed = urlparse(full)
        if parsed.scheme not in ("http","https"):
            continue
        host = parsed.netloc
        # skip famous hosts quickly
        if any(s in host for s in SKIP_HOSTS):
            continue
        if full not in seen:
            seen.add(full)
            orgs.append((text or host, full))
        if len(orgs) >= limit:
            break
    return orgs

def find_candidate_links(base_url, soup):
    anchors = soup.find_all("a", href=True)
    found = []
    for a in anchors:
        href = a['href'].strip()
        text = (a.get_text() or "").strip()
        low = (href + " " + text).lower()
        if any(k in low for k in KEYWORDS):
            full = urljoin(base_url, href)
            found.append((text or full, full))
    # dedupe preserve order
    seen = set(); uniq = []
    for t,u in found:
        if u not in seen:
            seen.add(u); uniq.append((t,u))
    return uniq

def extract_jobs_from_page(url):
    r = polite_get(url)
    if not r or r.status_code != 200:
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    jobs = []
    # quick heuristic: look for blocks mentioning deadline-like words
    blocks = soup.find_all(['article','li','tr','div','p'], limit=300)
    for b in blocks:
        text = b.get_text(" ", strip=True)
        if len(text) < 40: 
            continue
        low = text.lower()
        if any(x in low for x in ["last date","closing date","apply by","deadline","last date to apply","closing on","last date for"]):
            # try title
            title = None
            for tag in ['h1','h2','h3','h4','strong','b']:
                t = b.find(tag)
                if t and t.get_text(strip=True):
                    title = t.get_text(strip=True); break
            if not title:
                title = (text.split('.') or [text])[0][:140]
            link_tag = b.find('a', href=True)
            link = url if not link_tag else urljoin(url, link_tag['href'])
            snippet = text[:400]
            # simple date parse (best-effort)
            deadline = None
            try:
                import re, dateparser
                m = re.search(r'\\d{1,2}[ /.-]\\d{1,2}[ /.-]\\d{2,4}', text)
                if m:
                    d = dateparser.parse(m.group(0))
                    if d:
                        deadline = d.date().isoformat()
            except:
                pass
            jobs.append({'title': title, 'link': link, 'snippet': snippet, 'deadline': deadline})
    # also include pdf links with recruitment keywords
    for a in soup.find_all('a', href=True):
        href = a['href']
        if href.lower().endswith('.pdf') and any(k in (a.get_text() or href).lower() for k in ['notification','advertisement','recruit','vacancy','apply']):
            full = urljoin(url, href)
            title = a.get_text(strip=True) or full
            jobs.append({'title': title, 'link': full, 'snippet': 'PDF notification', 'deadline': None})
    return jobs

def store_result(org_name, org_url, job):
    results.append({
        'org_name': org_name,
        'org_url': org_url,
        'title': job.get('title'),
        'link': job.get('link'),
        'snippet': job.get('snippet'),
        'deadline': job.get('deadline'),
        'fetched_at': datetime.utcnow().isoformat()
    })

def crawl_worker(orgs):
    saved = 0
    for name, url in orgs:
        # skip if host in SKIP_HOSTS (double-check)
        if is_skippable(url):
            continue
        try:
            r = polite_get(url); time.sleep(REQUEST_SLEEP)
            if not r or r.status_code != 200:
                continue
            soup = BeautifulSoup(r.text, "html.parser")
            candidates = find_candidate_links(url, soup)
            # if none found, scan a few internal subpages (limited)
            if not candidates:
                anchors = soup.find_all('a', href=True)
                subs = []
                for a in anchors[:60]:
                    full = urljoin(url, a['href'])
                    if urlparse(full).netloc == urlparse(url).netloc:
                        subs.append(full)
                for sub in subs[:PER_ORG_SUBPAGES]:
                    r2 = polite_get(sub); time.sleep(REQUEST_SLEEP)
                    if not r2:
                        continue
                    s2 = BeautifulSoup(r2.text, "html.parser")
                    found = find_candidate_links(sub, s2)
                    if found:
                        candidates.extend(found)
            # visit each candidate page (limited)
            seen = set()
            for t, link in candidates[:PER_ORG_CANDIDATES]:
                if link in seen:
                    continue
                seen.add(link)
                jobs = extract_jobs_from_page(link)
                for job in jobs:
                    # if deadline present and expired, skip
                    try:
                        if job.get('deadline'):
                            if job['deadline'] < str(date.today()):
                                continue
                    except:
                        pass
                    store_result(name, url, job); saved += 1
                time.sleep(REQUEST_SLEEP)
        except Exception:
            continue
    return saved

# ---------- main full crawl ----------
crawl_thread = None
_stop_flag = False

def full_filtered_crawl():
    global _stop_flag
    orgs = fetch_orgs(limit=MAX_ORGS)
    # chunk to small batches
    batches = [orgs[i:i+10] for i in range(0, len(orgs), 10)]
    for batch in batches:
        if _stop_flag:
            break
        crawl_worker(batch)
    return True

# ---------- Flask endpoints ----------
@app.route('/')
def home():
    return "<h3>Gov Vacancy Finder (filtered) — use /start-filtered-crawl to begin. Check /status and /results</h3>"

@app.route('/start-filtered-crawl')
def start_filtered():
    global crawl_thread, _stop_flag
    if crawl_thread and crawl_thread.is_alive():
        return "Crawl already running", 400
    _stop_flag = False
    # clear previous results
    results.clear()
    crawl_thread = threading.Thread(target=full_filtered_crawl, daemon=True)
    crawl_thread.start()
    return "Crawl started", 200

@app.route('/stop-crawl')
def stop_crawl():
    global _stop_flag
    _stop_flag = True
    return "Stop requested", 200

@app.route('/status')
def status():
    running = crawl_thread.is_alive() if crawl_thread else False
    return jsonify({'running': running, 'found': len(results)})

@app.route('/results')
def results_endpoint():
    return jsonify(results)

@app.route('/download')
def download():
    if not results:
        return "No results yet", 404
    fname = f'gov_filtered_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    with open(fname, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['org_name','org_url','title','link','snippet','deadline','fetched_at'])
        for r in results:
            writer.writerow([r.get('org_name'), r.get('org_url'), r.get('title'), r.get('link'), r.get('snippet'), r.get('deadline'), r.get('fetched_at')])
    return send_file(fname, as_attachment=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
