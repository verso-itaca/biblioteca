#!/usr/bin/env python
# coding: utf-8

# In[1]:


# ===========================
# Biblio longform -> enrich (OPAC SBN -> Open Library -> LoC -> Internet Archive)
# TWO-PASS MODE:
#   Pass 1: standard cascade, strict threshold
#   Pass 2: only NOT_FOUND, more query variants + relaxed threshold
# Output: LONG enriched + WIDE enriched
# Key requirement: ext_url ALWAYS "human record page" (incl. OPAC SBN)
# ===========================

import os, re, json, time
from difflib import SequenceMatcher
from urllib.parse import quote_plus

import pandas as pd
import requests


import contextlib, signal

@contextlib.contextmanager
def hard_timeout(seconds):
    """Hard wall-clock timeout (POSIX). Interrupts DNS/connect hangs better than requests timeout."""
    if seconds is None or seconds <= 0:
        yield
        return
    # SIGALRM is available on macOS/Linux.
    try:
        old_handler = signal.getsignal(signal.SIGALRM)

        def _handler(signum, frame):
            raise TimeoutError(f"Hard timeout after {seconds}s")

        signal.signal(signal.SIGALRM, _handler)
        signal.setitimer(signal.ITIMER_REAL, float(seconds))
        try:
            yield
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0.0)
            signal.signal(signal.SIGALRM, old_handler)
    except Exception:
        # If signals are not available, fall back (requests timeout still applies).
        yield
# ---------------------------
# CONFIG
# ---------------------------
INPUT_CSV     = "biblio_longform_all_records.csv"
OUTPUT_LONG   = "biblio_longform_all_records_enriched.csv"
OUTPUT_WIDE   = "biblio_wide_records_enriched.csv"
PROGRESS_JSON = "biblio_enrich_progress.json"

TOPK_PER_SOURCE = 12
TIMEOUT = 12  # seconds (connect+read)
SBN_HARD_TIMEOUT = 6  # hard wall-clock cap for SBN requests
SBN_ENABLED = True  # auto-disabled if SBN endpoint is unreachable

PER_RECORD_BUDGET_PASS1 = 25  # seconds total across sources
PER_RECORD_BUDGET_PASS2 = 30  # seconds total across sources
SLEEP_BETWEEN_CALLS = 0.25

MIN_SCORE_KEEP = 0.12
MIN_SCORE_FOUND_1ST = 0.40     # first pass acceptance
MIN_SCORE_FOUND_2ND = 0.28     # second pass acceptance (ONLY for NOT_FOUND)

ALLOW_AUTHOR_ONLY_QUERIES = True
DEBUG_SBN = False

CONTACT_EMAIL = "pietro.terna@unito.it"
UA = f"biblio-enricher/5.0 (Jupyter; contact: {CONTACT_EMAIL})"

STANDARD_FIELDS = [
    "ext_source","ext_match_score","ext_title","ext_author","ext_year",
    "ext_publisher","ext_place","ext_language","ext_physical_desc",
    "ext_identifiers","ext_notes","ext_url"
]

SESSION = requests.Session()

def preflight_sbn():
    """Disable SBN if DNS/connect hangs or endpoint unreachable."""
    global SBN_ENABLED
    test_url = "https://opac.sbn.it/opacmobilegw/search.json?any=test&type=0&start=0&rows=1"
    try:
        with hard_timeout(2):
            r = SESSION.get(test_url, timeout=2)
        if r.status_code >= 400:
            SBN_ENABLED = False
    except Exception:
        SBN_ENABLED = False

preflight_sbn()

SESSION.headers.update({"User-Agent": UA})

def _sleep():
    time.sleep(SLEEP_BETWEEN_CALLS)

# ---------------------------
# Helpers
# ---------------------------
def norm(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[“”\"'’`]", "", s)
    s = re.sub(r"[^0-9a-zàèéìòùçäëïöüßñ \-:/]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def sim(a: str, b: str) -> float:
    a, b = norm(a), norm(b)
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()

def extract_year(s: str):
    if not s:
        return None
    m = re.search(r"(1[5-9]\d{2}|20\d{2})", str(s))
    return int(m.group(1)) if m else None

def normalize_year_from_data_pub(data_pub: str):
    return extract_year(data_pub)

def parse_series(collezion: str):
    if not collezion:
        return ("", "")
    s = str(collezion).strip()
    m = re.search(r"\bN\.?\s*(\d+)\b", s, flags=re.IGNORECASE)
    if m:
        no = m.group(1)
        series = re.sub(r"\bN\.?\s*\d+\b", "", s, flags=re.IGNORECASE).strip(" ,;-")
        return (series.strip(), no)
    return (s, "")

def pack_identifiers(d: dict) -> str:
    return json.dumps(d or {}, ensure_ascii=False)

def safe_list(v):
    if v is None:
        return []
    if isinstance(v, list):
        return v
    return [v]

def first_nonempty(vals):
    for v in vals:
        if v is None:
            continue
        s = str(v).strip()
        if s and s.lower() != "nan":
            return s
    return ""

def load_progress():
    if os.path.exists(PROGRESS_JSON):
        with open(PROGRESS_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"done_n_scheda": [], "last_book_number": 0}

def save_progress(prog):
    with open(PROGRESS_JSON, "w", encoding="utf-8") as f:
        json.dump(prog, f, ensure_ascii=False, indent=2)

# ---------------------------
# OPAC SBN "page url"
# ---------------------------
def sbn_bid_to_page_url(bid: str) -> str:
    if not isinstance(bid, str) or not bid.strip():
        return ""
    bid = bid.strip()
    m = re.match(r"^IT\\ICCU\\([A-Za-z0-9]{3})\\([0-9]{1,10})$", bid)
    if m:
        polo = m.group(1).upper()
        num = m.group(2).zfill(7)
        return f"https://opac.sbn.it/bid/{polo}{num}"
    bid_clean = bid.replace("\\", "")
    return f"https://opac.sbn.it/bid/{bid_clean}"

# ---------------------------
# SBN helpers
# ---------------------------
def sbn_clean_query(s: str) -> str:
    s = (s or "")
    s = re.sub(r"[\"'’`]", "", s)
    s = re.sub(r"[,.;:()\\[\\]{}<>|]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def sbn_clean_author(a: str) -> str:
    # remove common honorifics/titles and trailing qualifiers
    a = (a or "")
    a = re.sub(r"\b(card\.?|arciv\.?|mons\.?|rev\.?|s\.j\.?|o\.p\.?)\b", " ", a, flags=re.IGNORECASE)
    a = re.sub(r"\s+", " ", a).strip()
    return a

def sbn_clean_title(t: str) -> str:
    t = (t or "")
    t = re.sub(r"\s*/\s*.*$", "", t)  # drop anything after " / " (often author)
    t = re.sub(r"\s*:\s*.*$", "", t)  # optionally shorten after ":" for very long titles
    t = re.sub(r"\s+", " ", t).strip()
    return t

def sbn_extract_hits(js):
    if not isinstance(js, dict):
        return []
    for k in ["briefRecords", "records", "record", "results", "elenco", "items", "docs"]:
        v = js.get(k)
        if isinstance(v, list):
            return v
    for k in ["result", "response", "data"]:
        v = js.get(k)
        if isinstance(v, dict):
            for k2 in ["briefRecords", "records", "items", "docs", "results", "elenco"]:
                v2 = v.get(k2)
                if isinstance(v2, list):
                    return v2
    return []

def series_bonus(book, text):
    series = norm(book.get("series",""))
    s_no   = norm(book.get("series_no",""))
    t = norm(text)
    bonus = 0.0
    if series and series in t:
        bonus += 0.10
    if s_no and re.search(rf"\b{s_no}\b", t):
        bonus += 0.06
    return bonus

# ---------------------------
# Load input
# ---------------------------
df = pd.read_csv(INPUT_CSV)

# Support both historical longform layouts:
#   OLD: columns = n_scheda, campo, valore
#   NEW: columns = field, value, with "n_scheda" emitted as a row that starts each record
if {"n_scheda","campo","valore"}.issubset(df.columns):
    df["n_scheda"] = pd.to_numeric(df["n_scheda"], errors="coerce")
    if df["n_scheda"].isna().any():
        bad = df.loc[df["n_scheda"].isna()].head(10)
        raise ValueError(f"n_scheda non numerico in alcune righe (prime 10):\n{bad}")
    df["n_scheda"] = df["n_scheda"].astype(int)
    df["campo"] = df["campo"].astype(str)
    df["valore"] = df["valore"].astype(str)

elif {"field","value"}.issubset(df.columns):
    # Normalize column names used downstream
    df["campo"] = df["field"].astype(str)
    df["valore"] = df["value"].astype(str)

    # Reconstruct n_scheda if it is encoded as a "field" row
    if "n_scheda" not in df.columns:
        mask_ns = df["campo"].str.strip().str.lower().eq("n_scheda")
        if not mask_ns.any():
            raise ValueError(
                "CSV nel formato field/value ma senza colonna n_scheda e senza righe con field='n_scheda'. "
                f"Colonne trovate={list(df.columns)}"
            )
        df["n_scheda"] = pd.to_numeric(df["valore"].where(mask_ns), errors="coerce").ffill()
        if df["n_scheda"].isna().any():
            bad = df.loc[df["n_scheda"].isna()].head(10)
            raise ValueError(f"Impossibile ricostruire n_scheda (prime 10 righe problematiche):\n{bad}")
        df["n_scheda"] = df["n_scheda"].astype(int)
        # Drop the structural rows that only carry n_scheda
        df = df.loc[~mask_ns].copy()
    else:
        df["n_scheda"] = pd.to_numeric(df["n_scheda"], errors="coerce").astype("Int64")
        if df["n_scheda"].isna().any():
            bad = df.loc[df["n_scheda"].isna()].head(10)
            raise ValueError(f"n_scheda non numerico in alcune righe (prime 10):\n{bad}")
        df["n_scheda"] = df["n_scheda"].astype(int)

else:
    raise ValueError(
        f"CSV non nel formato atteso. Colonne trovate={list(df.columns)}; "
        "attesi (n_scheda,campo,valore) oppure (field,value) con righe n_scheda."
    )

groups = list(df.groupby("n_scheda", sort=True))
n_books = len(groups)

def book_input_from_group(g: pd.DataFrame) -> dict:
    rec = {"n_scheda": int(g["n_scheda"].iloc[0])}

    def fv(field):
        vals = g.loc[g["campo"] == field, "valore"].tolist()
        return first_nonempty(vals)

    rec["autore"]    = fv("autore")
    rec["titolo"]    = fv("titolo")
    rec["editore"]   = fv("editore")
    rec["luogo_pub"] = fv("luogo_pub")
    rec["data_pub"]  = fv("data_pub")
    rec["cod_isbn"]  = fv("cod_isbn")
    rec["collezion"] = fv("collezion")

    rec["year"] = normalize_year_from_data_pub(rec["data_pub"]) or extract_year(rec["titolo"])
    series, series_no = parse_series(rec["collezion"])
    rec["series"] = series
    rec["series_no"] = series_no
    return rec

def build_queries_pass1(book: dict):
    titolo  = (book.get("titolo") or "").strip()
    autore  = (book.get("autore") or "").strip()
    editore = (book.get("editore") or "").strip()
    year    = book.get("year")
    series  = (book.get("series") or "").strip()
    s_no    = (book.get("series_no") or "").strip()

    queries = []
    if titolo:
        q1 = " ".join([x for x in [titolo, autore, str(year) if year else ""] if x]).strip()
        q2 = " ".join([x for x in [titolo, autore] if x]).strip()
        q3 = " ".join([x for x in [titolo, autore, editore, str(year) if year else ""] if x]).strip()
        for q in [q1, q2, q3]:
            if q and q not in queries:
                queries.append(q)
        return queries

    # no title
    qA = " ".join([x for x in [autore, series, s_no, editore, str(year) if year else ""] if x]).strip()
    qB = " ".join([x for x in [autore, series, s_no] if x]).strip()
    qC = " ".join([x for x in [autore, series] if x]).strip()
    qD = " ".join([x for x in [autore, str(year) if year else "", editore] if x]).strip()

    for q in [qA, qB, qC, qD]:
        if q and q not in queries:
            queries.append(q)

    if ALLOW_AUTHOR_ONLY_QUERIES and autore and autore not in queries:
        queries.append(autore)

    return queries

def build_queries_pass2(book: dict):
    """
    More aggressive variants (only used for NOT_FOUND).
    """
    titolo  = (book.get("titolo") or "").strip()
    autore  = (book.get("autore") or "").strip()
    editore = (book.get("editore") or "").strip()
    luogo   = (book.get("luogo_pub") or "").strip()
    year    = book.get("year")
    series  = (book.get("series") or "").strip()
    s_no    = (book.get("series_no") or "").strip()

    autore2 = sbn_clean_author(autore)
    titolo2 = sbn_clean_title(titolo)

    variants = []

    # title present: include cleaned title/author variants, and drop publisher/year variants too
    if titolo:
        base = [
            " ".join([x for x in [titolo, autore] if x]),
            " ".join([x for x in [titolo2, autore] if x]),
            " ".join([x for x in [titolo2, autore2] if x]),
            " ".join([x for x in [titolo2, autore2, str(year) if year else ""] if x]),
            " ".join([x for x in [titolo2, editore] if x]),
            " ".join([x for x in [titolo2, luogo] if x]),
        ]
        for q in base:
            q = q.strip()
            if q and q not in variants:
                variants.append(q)
        # author-only as last resort
        if ALLOW_AUTHOR_ONLY_QUERIES and autore2 and autore2 not in variants:
            variants.append(autore2)
        return variants

    # no title: lean on series / number / publisher / place
    base = [
        " ".join([x for x in [autore2, series, s_no] if x]),
        " ".join([x for x in [series, s_no, editore] if x]),
        " ".join([x for x in [autore2, editore, luogo] if x]),
        " ".join([x for x in [autore2, str(year) if year else "", editore] if x]),
        " ".join([x for x in [series, editore] if x]),
        " ".join([x for x in [autore2] if x]),
    ]
    for q in base:
        q = q.strip()
        if q and q not in variants:
            variants.append(q)
    return variants

# ---------------------------
# OPAC SBN API
# ---------------------------
def sbn_search_any(q: str, rows=TOPK_PER_SOURCE):
    if not SBN_ENABLED:
        return None
    url = f"https://opac.sbn.it/opacmobilegw/search.json?any={quote_plus(q)}&type=0&start=0&rows={rows}"
    #with hard_timeout(SBN_HARD_TIMEOUT): #ptptpt
    with hard_timeout(SBN_HARD_TIMEOUT):
        r = SESSION.get(url, timeout=TIMEOUT)
    _sleep()
    r.raise_for_status()
    return r.json()

def sbn_search_isbn(isbn: str, rows=TOPK_PER_SOURCE):
    url = f"https://opac.sbn.it/opacmobilegw/search.json?isbn={quote_plus(isbn)}&start=0&rows={rows}"
    #with hard_timeout(SBN_HARD_TIMEOUT): #ptptpt
    with hard_timeout(SBN_HARD_TIMEOUT):
        r = SESSION.get(url, timeout=TIMEOUT)
    _sleep()
    r.raise_for_status()
    return r.json()

def sbn_full(bid: str):
    url = f"https://opac.sbn.it/opacmobilegw/full.json?bid={quote_plus(bid)}"
    #with hard_timeout(SBN_HARD_TIMEOUT): #ptptpt
    with hard_timeout(SBN_HARD_TIMEOUT):
        r = SESSION.get(url, timeout=TIMEOUT)
    _sleep()
    r.raise_for_status()
    return r.json()

def _score_sbn_hits(book, hits):
    titolo_in = (book.get("titolo") or "").strip()
    autore_in = sbn_clean_author(book.get("autore") or "").strip()
    year_in   = book.get("year")

    cands = []
    for h in (hits or [])[:TOPK_PER_SOURCE]:
        title = h.get("titolo") or ""
        auth  = h.get("autorePrincipale") or ""
        pub   = h.get("pubblicazione") or ""
        bid   = h.get("codiceIdentificativo") or ""
        by = extract_year(pub)

        if titolo_in:
            score = 0.70 * sim(titolo_in, title) + 0.30 * sim(autore_in, auth)
        else:
            score = 0.85 * sim(autore_in, auth) + 0.15 * (0.2 if title else 0.0)

        score += series_bonus(book, f"{title} {pub}")

        if year_in and by and abs(year_in - by) <= 1:
            score += 0.06

        if score >= MIN_SCORE_KEEP:
            cands.append({"source":"OPAC SBN","score":score,"title":title,"author":auth,"pub":pub,"bid":bid,"raw":h})

    cands.sort(key=lambda x: x["score"], reverse=True)

    for c in cands[:2]:
        if c.get("bid"):
            try:
                c["full"] = sbn_full(c["bid"])
            except Exception:
                c["full"] = None
    return cands

def sbn_candidates(book: dict, pass_no: int):
    isbn = (book.get("cod_isbn") or "").strip()

    queries = build_queries_pass1(book) if pass_no == 1 else build_queries_pass2(book)

    # ISBN-first in both passes
    if isbn:
        try:
            js = sbn_search_isbn(isbn)
            hits = sbn_extract_hits(js)
            if DEBUG_SBN:
                print(f"   SBN[isbn] numFound={js.get('numFound')} hits={len(hits)}")
            c = _score_sbn_hits(book, hits)
            if c:
                return c
        except Exception:
            pass

    # any= variants
    for q in queries:
        if not q:
            continue
        for qv in [q, sbn_clean_query(q)]:
            if not qv:
                continue
            try:
                js = sbn_search_any(qv)
                hits = sbn_extract_hits(js)
                if DEBUG_SBN:
                    print(f"   SBN[any] hits={len(hits)} q='{qv[:70]}'")
                c = _score_sbn_hits(book, hits)
                if c:
                    return c
            except Exception:
                continue

    return []

# ---------------------------
# Open Library
# ---------------------------
def ol_search_q(q: str, rows=TOPK_PER_SOURCE):
    params = {"q": q, "limit": rows}
    r = SESSION.get("https://openlibrary.org/search.json", params=params, timeout=TIMEOUT)
    _sleep()
    r.raise_for_status()
    return r.json()

def ol_candidates(book: dict, pass_no: int):
    isbn = (book.get("cod_isbn") or "").strip()
    queries = []
    if isbn:
        queries.append(f"isbn:{isbn}")
    queries.extend(build_queries_pass1(book) if pass_no == 1 else build_queries_pass2(book))

    titolo_in = (book.get("titolo") or "").strip()
    autore_in = sbn_clean_author(book.get("autore") or "").strip()
    year_in   = book.get("year")

    for q in queries:
        if not q:
            continue
        try:
            js = ol_search_q(q)
        except Exception:
            continue

        docs = js.get("docs", [])[:TOPK_PER_SOURCE]
        cands = []
        for d in docs:
            title = d.get("title") or ""
            auth  = (d.get("author_name") or [""])[0]
            year  = d.get("first_publish_year")
            pub   = (d.get("publisher") or [""])[0] if isinstance(d.get("publisher"), list) else (d.get("publisher") or "")
            place = (d.get("publish_place") or [""])[0] if isinstance(d.get("publish_place"), list) else (d.get("publish_place") or "")
            lang  = (d.get("language") or [""])[0] if isinstance(d.get("language"), list) else (d.get("language") or "")
            key   = d.get("key","")

            if titolo_in:
                score = 0.70 * sim(titolo_in, title) + 0.30 * sim(autore_in, auth)
            else:
                score = 0.85 * sim(autore_in, auth) + 0.15 * (0.2 if title else 0.0)

            score += series_bonus(book, f"{title} {pub} {place}")

            if year_in and year and abs(year_in - int(year)) <= 1:
                score += 0.05

            if score >= MIN_SCORE_KEEP:
                cands.append({"source":"Open Library","score":score,"title":title,"author":auth,"year":year,
                              "publisher":pub,"place":place,"language":lang,"key":key,"raw":d})

        cands.sort(key=lambda x: x["score"], reverse=True)
        if cands:
            return cands

    return []

# ---------------------------
# Library of Congress
# ---------------------------
def loc_search(q: str, rows=TOPK_PER_SOURCE):
    params = {"fo":"json", "q": q, "c": rows}
    r = SESSION.get("https://www.loc.gov/books/", params=params, timeout=TIMEOUT)
    _sleep()
    r.raise_for_status()
    return r.json()

def loc_candidates(book: dict, pass_no: int):
    queries = build_queries_pass1(book) if pass_no == 1 else build_queries_pass2(book)
    titolo_in = (book.get("titolo") or "").strip()
    autore_in = sbn_clean_author(book.get("autore") or "").strip()
    year_in   = book.get("year")

    for q in queries:
        if not q:
            continue
        try:
            js = loc_search(q)
        except Exception:
            continue

        results = js.get("results", [])[:TOPK_PER_SOURCE]
        cands = []
        for it in results:
            title = it.get("title") or ""
            auth_list = it.get("contributor") or it.get("creator") or []
            auth = auth_list[0] if isinstance(auth_list, list) and auth_list else ""
            date = it.get("date") or ""
            year = extract_year(date)
            loc_id = it.get("id") or ""

            if titolo_in:
                score = 0.70 * sim(titolo_in, title) + 0.30 * sim(autore_in, auth)
            else:
                score = 0.85 * sim(autore_in, auth) + 0.15 * (0.2 if title else 0.0)

            score += series_bonus(book, f"{title} {date}")

            if year_in and year and abs(year_in - year) <= 1:
                score += 0.05

            if score >= MIN_SCORE_KEEP:
                cands.append({"source":"Library of Congress","score":score,"title":title,"author":auth,"date":date,"id":loc_id,"raw":it})

        cands.sort(key=lambda x: x["score"], reverse=True)
        if cands:
            return cands

    return []

# ---------------------------
# Internet Archive
# ---------------------------
def ia_advanced_search(title: str, author: str, q_extra: str = "", rows=TOPK_PER_SOURCE):
    q = []
    if title:
        q.append(f'title:("{title}")')
    if author:
        q.append(f'creator:("{author}")')
    if q_extra:
        q.append(q_extra)
    q.append('(mediatype:texts OR collection:opensource_texts)')
    query = " AND ".join([x for x in q if x])

    params = {
        "q": query,
        "fl[]": ["identifier", "title", "creator", "date", "publisher", "language"],
        "rows": rows,
        "page": 1,
        "output": "json"
    }
    r = SESSION.get("https://archive.org/advancedsearch.php", params=params, timeout=TIMEOUT)
    _sleep()
    r.raise_for_status()
    return r.json()

def ia_metadata(identifier: str):
    r = SESSION.get(f"https://archive.org/metadata/{identifier}", timeout=TIMEOUT)
    _sleep()
    r.raise_for_status()
    return r.json()

def ia_candidates(book: dict, pass_no: int):
    titolo_in = (book.get("titolo") or "").strip()
    autore_in = sbn_clean_author(book.get("autore") or "").strip()
    year_in   = book.get("year")
    queries = build_queries_pass1(book) if pass_no == 1 else build_queries_pass2(book)

    cands_all = []
    for q in queries[:5 if pass_no==2 else 3]:
        try:
            js = ia_advanced_search(titolo_in if titolo_in else "", autore_in if autore_in else "", q_extra=q)
        except Exception:
            continue

        docs = (js.get("response", {}).get("docs", []) or [])[:TOPK_PER_SOURCE]
        for d in docs:
            title = d.get("title") or ""
            creator = d.get("creator")
            auth = creator[0] if isinstance(creator, list) and creator else (creator or "")
            year = extract_year(d.get("date"))
            ident = d.get("identifier")

            if titolo_in:
                score = 0.70 * sim(titolo_in, title) + 0.30 * sim(autore_in, auth)
            else:
                score = 0.85 * sim(autore_in, auth) + 0.15 * (0.2 if title else 0.0)

            score += series_bonus(book, f"{title} {d.get('publisher','')}")

            if year_in and year and abs(year_in - year) <= 1:
                score += 0.05

            if score >= MIN_SCORE_KEEP:
                cands_all.append({"source":"Internet Archive","score":score,"title":title,"author":auth,"year":year,
                                  "identifier":ident,"raw":d})

        if cands_all:
            break

    cands_all.sort(key=lambda x: x["score"], reverse=True)

    if cands_all and cands_all[0].get("identifier"):
        try:
            cands_all[0]["full"] = ia_metadata(cands_all[0]["identifier"])
        except Exception:
            cands_all[0]["full"] = None

    return cands_all

# ---------------------------
# Map to ext_* (ext_url always human page)
# ---------------------------
def from_sbn(best: dict) -> dict:
    full = best.get("full") or {}
    bid = best.get("bid","")
    return {
        "ext_source": "OPAC SBN",
        "ext_match_score": f"{best.get('score',0):.3f}",
        "ext_title": full.get("titolo") or best.get("title") or "",
        "ext_author": full.get("autorePrincipale") or best.get("author") or "",
        "ext_year": str(extract_year(full.get("pubblicazione")) or extract_year(best.get("pub")) or "" or ""),
        "ext_publisher": "",
        "ext_place": "",
        "ext_language": full.get("linguaPubblicazione") or "",
        "ext_physical_desc": full.get("descrizioneFisica") or "",
        "ext_identifiers": pack_identifiers({"bid": bid}),
        "ext_notes": " | ".join(safe_list(full.get("note"))) if full.get("note") else "",
        "ext_url": sbn_bid_to_page_url(bid)
    }

def from_ol(best: dict) -> dict:
    d = best.get("raw") or {}
    identifiers = {}
    for k in ["isbn", "oclc", "lccn"]:
        if k in d and d[k]:
            identifiers[k] = d[k][:10] if isinstance(d[k], list) else d[k]
    key = best.get("key","") or d.get("key","")
    return {
        "ext_source": "Open Library",
        "ext_match_score": f"{best.get('score',0):.3f}",
        "ext_title": best.get("title") or "",
        "ext_author": best.get("author") or "",
        "ext_year": str(best.get("year") or ""),
        "ext_publisher": best.get("publisher") or "",
        "ext_place": best.get("place") or "",
        "ext_language": best.get("language") or "",
        "ext_physical_desc": "",
        "ext_identifiers": pack_identifiers(identifiers),
        "ext_notes": "",
        "ext_url": f"https://openlibrary.org{key}" if key else ""
    }

def from_loc(best: dict) -> dict:
    return {
        "ext_source": "Library of Congress",
        "ext_match_score": f"{best.get('score',0):.3f}",
        "ext_title": best.get("title") or "",
        "ext_author": best.get("author") or "",
        "ext_year": str(extract_year(best.get("date") or "") or ""),
        "ext_publisher": "",
        "ext_place": "",
        "ext_language": "",
        "ext_physical_desc": "",
        "ext_identifiers": pack_identifiers({"loc_id": best.get("id")}),
        "ext_notes": "",
        "ext_url": best.get("id") or ""
    }

def from_ia(best: dict) -> dict:
    full = best.get("full") or {}
    md = full.get("metadata") or {}

    def md1(k):
        v = md.get(k, "")
        if isinstance(v, list):
            return v[0] if v else ""
        return v or ""

    ident = best.get("identifier") or ""
    return {
        "ext_source": "Internet Archive",
        "ext_match_score": f"{best.get('score',0):.3f}",
        "ext_title": md1("title") or best.get("title") or "",
        "ext_author": md1("creator") or best.get("author") or "",
        "ext_year": str(extract_year(md1("date")) or best.get("year") or ""),
        "ext_publisher": md1("publisher"),
        "ext_place": md1("publishplace"),
        "ext_language": md1("language"),
        "ext_physical_desc": md1("description"),
        "ext_identifiers": pack_identifiers({"identifier": ident}),
        "ext_notes": md1("notes"),
        "ext_url": f"https://archive.org/details/{ident}" if ident else ""
    }

def choose_best_candidate(book: dict, pass_no: int):
    """Select best hit across sources with a hard per-record time budget.

    This prevents the run from appearing 'stuck' on a single n_scheda when one or more
    upstream services are slow or intermittently unavailable.
    """
    threshold = MIN_SCORE_FOUND_1ST if pass_no == 1 else MIN_SCORE_FOUND_2ND
    budget = PER_RECORD_BUDGET_PASS1 if pass_no == 1 else PER_RECORD_BUDGET_PASS2
    t0 = time.time()

    def time_left():
        return budget - (time.time() - t0)

    def safe(label, fn):
        try:
            if time_left() <= 0:
                return []
            return fn()
        except Exception as e:
            # Do not fail the whole run on a single upstream issue
            if DEBUG:
                print(f"  [WARN] {label} failed: {type(e).__name__}: {e}")
            return []

    # Try sources in priority order; stop early if budget exhausted
    sbn = safe("SBN", lambda: sbn_candidates(book, pass_no))
    if sbn and sbn[0]["score"] >= threshold:
        return sbn[0], from_sbn(sbn[0])

    ol = safe("OpenLibrary", lambda: ol_candidates(book, pass_no))
    if ol and ol[0]["score"] >= threshold:
        return ol[0], from_ol(ol[0])

    loc = safe("LoC", lambda: loc_candidates(book, pass_no))
    if loc and loc[0]["score"] >= threshold:
        return loc[0], from_loc(loc[0])

    ia = safe("IA", lambda: ia_candidates(book, pass_no))
    if ia and ia[0]["score"] >= threshold:
        return ia[0], from_ia(ia[0])

    # If we ran out of time (or nothing matched), mark as NOT_FOUND
    return None, {
        "ext_source":"NOT_FOUND","ext_match_score":"0.000",
        "ext_title":"","ext_author":"","ext_year":"",
        "ext_publisher":"","ext_place":"","ext_language":"",
        "ext_physical_desc":"","ext_identifiers":pack_identifiers({}),
        "ext_notes":"","ext_url":""
    }

# ---------------------------
# RUN PASS 1 (resumable)
# ---------------------------
prog = load_progress()
done = set(int(x) for x in prog.get("done_n_scheda", []))

if os.path.exists(OUTPUT_LONG):
    out_df = pd.read_csv(OUTPUT_LONG)
    out_df["n_scheda"] = out_df["n_scheda"].astype(int)
    out_df["campo"] = out_df["campo"].astype(str)
    out_df["valore"] = out_df["valore"].astype(str)
else:
    out_df = df.copy()

already_ext = set()
mask_ext = out_df["campo"].astype(str).str.startswith("ext_")
for r in out_df.loc[mask_ext, ["n_scheda","campo"]].itertuples(index=False):
    already_ext.add((int(r.n_scheda), str(r.campo)))

added_rows = []

found_count = 0
notfound_count = 0
found_by_source = {"OPAC SBN": 0, "Open Library": 0, "Library of Congress": 0, "Internet Archive": 0}

for book_number, (n_scheda, g) in enumerate(groups, start=1):
    print(f"[PASS1 {book_number}/{n_books}] n_scheda={n_scheda}")
    if int(n_scheda) in done:
        continue

    book = book_input_from_group(g)
    _, ext = choose_best_candidate(book, pass_no=1)
    src = ext.get("ext_source")

    if src != "NOT_FOUND":
        found_count += 1
        if src in found_by_source:
            found_by_source[src] += 1
    else:
        notfound_count += 1

    for k in STANDARD_FIELDS:
        key = (int(n_scheda), k)
        if key in already_ext:
            continue
        added_rows.append({"n_scheda": int(n_scheda), "campo": k, "valore": ext.get(k,"")})
        already_ext.add(key)

    done.add(int(n_scheda))
    prog["done_n_scheda"] = sorted(done)
    prog["last_book_number"] = book_number
    save_progress(prog)

    if book_number % 10 == 0:
        if added_rows:
            out_df = pd.concat([out_df, pd.DataFrame(added_rows)], ignore_index=True)
            added_rows = []
        out_df.to_csv(OUTPUT_LONG, index=False)
        print(
            f"Checkpoint PASS1: {OUTPUT_LONG} | "
            f"found={found_count} not_found={notfound_count} | "
            f"SBN={found_by_source['OPAC SBN']} "
            f"OL={found_by_source['Open Library']} "
            f"LoC={found_by_source['Library of Congress']} "
            f"IA={found_by_source['Internet Archive']}"
        )

if added_rows:
    out_df = pd.concat([out_df, pd.DataFrame(added_rows)], ignore_index=True)
out_df.to_csv(OUTPUT_LONG, index=False)

print(
    f"PASS1 completed. LONG: {OUTPUT_LONG} | "
    f"found={found_count} not_found={notfound_count} | "
    f"SBN={found_by_source['OPAC SBN']} "
    f"OL={found_by_source['Open Library']} "
    f"LoC={found_by_source['Library of Congress']} "
    f"IA={found_by_source['Internet Archive']}"
)

# ---------------------------
# PASS 2: ONLY NOT_FOUND (overwrite ext_* for those)
# ---------------------------
# Identify NOT_FOUND from the CURRENT long
df_ext = out_df[out_df["campo"]=="ext_source"][["n_scheda","valore"]].copy()
df_ext["n_scheda"] = df_ext["n_scheda"].astype(int)
not_found_ids = set(df_ext.loc[df_ext["valore"]=="NOT_FOUND", "n_scheda"].tolist())

print(f"PASS2 starting: NOT_FOUND records = {len(not_found_ids)}")

# Build a quick lookup from original input groups
group_map = {int(n): g for (n, g) in groups}

# Remove existing ext_* rows for NOT_FOUND (so we can rewrite them cleanly)
mask_drop = out_df["n_scheda"].isin(list(not_found_ids)) & out_df["campo"].astype(str).str.startswith("ext_")
out_df = out_df.loc[~mask_drop].copy()

# Rebuild already_ext after drop
already_ext = set()
mask_ext = out_df["campo"].astype(str).str.startswith("ext_")
for r in out_df.loc[mask_ext, ["n_scheda","campo"]].itertuples(index=False):
    already_ext.add((int(r.n_scheda), str(r.campo)))

added_rows = []

pass2_found = 0
pass2_by_source = {"OPAC SBN": 0, "Open Library": 0, "Library of Congress": 0, "Internet Archive": 0}
pass2_still_not_found = 0

for idx, n_scheda in enumerate(sorted(not_found_ids), start=1):
    g = group_map.get(int(n_scheda))
    if g is None:
        continue
    print(f"[PASS2 {idx}/{len(not_found_ids)}] n_scheda={n_scheda}")

    book = book_input_from_group(g)
    _, ext = choose_best_candidate(book, pass_no=2)
    src = ext.get("ext_source")

    if src != "NOT_FOUND":
        pass2_found += 1
        if src in pass2_by_source:
            pass2_by_source[src] += 1
    else:
        pass2_still_not_found += 1

    for k in STANDARD_FIELDS:
        key = (int(n_scheda), k)
        if key in already_ext:
            continue
        added_rows.append({"n_scheda": int(n_scheda), "campo": k, "valore": ext.get(k,"")})
        already_ext.add(key)

    if idx % 10 == 0:
        if added_rows:
            out_df = pd.concat([out_df, pd.DataFrame(added_rows)], ignore_index=True)
            added_rows = []
        out_df.to_csv(OUTPUT_LONG, index=False)
        print(
            f"Checkpoint PASS2: {OUTPUT_LONG} | "
            f"pass2_found={pass2_found} pass2_still_not_found={pass2_still_not_found} | "
            f"SBN={pass2_by_source['OPAC SBN']} "
            f"OL={pass2_by_source['Open Library']} "
            f"LoC={pass2_by_source['Library of Congress']} "
            f"IA={pass2_by_source['Internet Archive']}"
        )

if added_rows:
    out_df = pd.concat([out_df, pd.DataFrame(added_rows)], ignore_index=True)

out_df.to_csv(OUTPUT_LONG, index=False)

print(
    f"PASS2 completed. LONG: {OUTPUT_LONG} | "
    f"pass2_found={pass2_found} pass2_still_not_found={pass2_still_not_found} | "
    f"SBN={pass2_by_source['OPAC SBN']} "
    f"OL={pass2_by_source['Open Library']} "
    f"LoC={pass2_by_source['Library of Congress']} "
    f"IA={pass2_by_source['Internet Archive']}"
)

# ---------------------------
# LONG -> WIDE
# ---------------------------
df_long = out_df.copy()
df_long["valore"] = df_long["valore"].fillna("").astype(str)
df_long.loc[df_long["campo"] == "n_scheda", "campo"] = "n_scheda_field"

agg = (df_long
       .groupby(["n_scheda","campo"])["valore"]
       .agg(lambda s: " | ".join([v.strip() for v in s if isinstance(v,str) and v.strip()]))
       .reset_index())

df_wide = agg.pivot(index="n_scheda", columns="campo", values="valore").reset_index()

front = ["n_scheda","titolo","autore","data_pub","luogo_pub","editore","collezion",
         "ext_source","ext_match_score","ext_title","ext_author","ext_year","ext_url","ext_identifiers","n_scheda_field"]
cols = [c for c in front if c in df_wide.columns] + [c for c in df_wide.columns if c not in front]
df_wide = df_wide[cols]

df_wide.to_csv(OUTPUT_WIDE, index=False)
print(f"WIDE creato: {OUTPUT_WIDE} | righe={len(df_wide)} colonne={len(df_wide.columns)}")


# ---------------------------
# mail
# ---------------------------

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Configurazione
sender = "pietro.terna@gmail.com"
receiver = "pietro.terna@unito.it"
password = "dqxj ihqe icnn eppr" # Usa una Password per le App
smtp_server = "smtp.gmail.com"
port = 587

# Creazione messaggio
msg = MIMEMultipart()
msg['From'] = sender
msg['To'] = receiver
msg['Subject'] = "fine programma"
body = "Fine programma, avviso da Ryzen"
msg.attach(MIMEText(body, 'plain'))

# Invio
try:
    server = smtplib.SMTP(smtp_server, port)
    server.starttls() # Criptazione
    server.login(sender, password)
    server.send_message(msg)
    server.quit()
    print("Email inviata con successo!")
except Exception as e:
    print(f"Errore: {e}")

