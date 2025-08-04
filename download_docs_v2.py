#!/usr/bin/env python3
"""
download_docs.py – fetch full content **and every attachment** for each USU KC
document listed in docs.csv.

Folder layout
-------------
dump/
├─ <guid>_<version>.html
└─ attachments/
   └─ <guid>/
      ├─ 108998_image2020-4-1_10-8-14.png
      ├─ 109001_Detailed-Spec.pdf
      └─ manifest.json
"""

import csv, os, pathlib, textwrap, re, requests, sys, json, html
import urllib.parse as up           
import html as html_lib             
from time import sleep

# ── 0.  Config -----------------------------------------------------------
APP_ID   = "3ea38aee-2df7-4cfe-a13d-a7161958bded"        
ENDPOINT = ("https://knowledge.revenue.nsw.gov.au/"
            "knowledgebase/services/DocumentService")
BASE_URL = "https://knowledge.revenue.nsw.gov.au/knowledgebase/"
PROXY    = "http://127.0.0.1:3128"                      # CNTLM relay
CA_FILE  = None                                       

COOKIES = {                               # captured from browser session
    "JSESSIONID":  "C3AF4C40DC78103AD2990D00FDA6AA1B",
    "GKSESSIONID": "XUJDgiAz2tXPjX1v21fttgMrfGmjGwjx",
    "GKREALM":     "defaulthost",
    "GKCSRFTOKEN": "BjWRl2xE9jIE065v58ElBvXIo47mOLM3",
}

DUMP     = pathlib.Path("dump")
ATT_ROOT = DUMP / "attachments"
PAUSE    = 1.0                 # Gap between downloads (seconds)

DOWNLOAD_ATTACHMENTS = False

DUMP.mkdir(exist_ok=True)
ATT_ROOT.mkdir(exist_ok=True)

# ── 1.  Session ----------------------------------------------------------
s = requests.Session()
s.proxies  = {"http": PROXY, "https": PROXY}
s.cookies.update(COOKIES)

def csrf() -> str:
    for c in reversed(list(s.cookies)):
        if c.name.upper() == "GKCSRFTOKEN":
            return c.value
    raise RuntimeError("CSRF cookie missing")

# # ── 2.  SOAP Payload -----------------------------------------------------
SOAP_ONE = textwrap.dedent(f"""\
    <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
                   xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                   xmlns:xsd="http://www.w3.org/2001/XMLSchema">
      <soap:Header></soap:Header>
      <soap:Body xmlns:ns0="http://ws.atlantis.usu.de/documentservice">
        <ns0:getDocumentsReq>
          <mandatorkey>DCS_REVENUE</mandatorkey>
          <language>en_US</language>
          <contentLanguage>en</contentLanguage>

          <parameters>
            <paramType>allEntities</paramType>
            <paramValue>true</paramValue>
            <paramOperator/>
          </parameters>

          <parameters>
            <paramType>docGUID</paramType>
            <paramValue>{{guid}}</paramValue>       
            <paramOperator/>
          </parameters>

          <withContent>true</withContent>
          <withMetaData>true</withMetaData>

          <!-- Only the meta you care about -->
          <additionalMeta><paramName>attachments</paramName><paramValue>panel</paramValue></additionalMeta>
          <additionalMeta><paramName>references</paramName><paramValue>true</paramValue></additionalMeta>
          <additionalMeta><paramName>versions</paramName><paramValue>true</paramValue></additionalMeta>

          <!-- comment out or drop the rest to save payload -->
          <!--
          <additionalMeta><paramName>history</paramName><paramValue>true</paramValue></additionalMeta>
          …etc…
          -->

          <sessionId/>
        </ns0:getDocumentsReq>
      </soap:Body>
    </soap:Envelope>""")

def fetch_html(guid: str, *, download_atts: bool = True):
    """
    POST the SOAP envelope and return (html, props, manifest).

    • html      – the document body returned by USU  
    • props     – {propName: value, …} extracted from the payload  
    • manifest  – [{att_id, file_name, url, saved_as}, …] for each attachment
                  (empty list if none or download_atts=False)

    Raises RuntimeError on any response that isn’t usable JSON.
    """
    body = SOAP_ONE.format(guid=guid)

    r = s.post(
        ENDPOINT,
        data=body.encode(),
        headers={
            "Content-Type":  "text/xml; charset=UTF-8",
            "Accept":        "application/json",
            "responsetype":  "json",
            "GK-CSRF-Token": csrf(),
        },
        timeout=45,
        verify=CA_FILE or False, 
    )
    r.raise_for_status()

    # ── safe JSON load ───────────────────────────────────────────────────
    try:
        payload = r.json()
    except ValueError as e:
        snippet = r.text[:400].replace("\n", " ")
        raise RuntimeError(f"Expected JSON, got something else: {snippet} …") from e

    docs = payload["Envelope"]["Body"]["getDocumentsRes"]["documents"]
    if isinstance(docs, dict):                       # USU returns dict for 1 doc
        docs = [docs]
    if not docs:
        raise RuntimeError("No 'documents' array in payload")

    doc   = docs[0]
    html  = doc["content"]
    props = {p["name"]: p["value"] for p in doc["properties"]}

    # ── attachment download (optional) ───────────────────────────────────
    manifest: list[dict] = []
    if download_atts:
        att_dir = ATT_ROOT / guid          # e.g. dump/attachments/<guid>/
        for rec in find_attachment_links(html):
            safe_name = f"{rec['att_id']}_{slugify(rec['file_name'])}"
            dest      = att_dir / safe_name
            if not dest.exists():          # skip if already on disk
                try:
                    download_attachment(rec, dest)
                    print(f"  ↳ saved {dest.relative_to(DUMP)}")
                except Exception as e:
                    print(f"  ↳ failed {rec['file_name']}: {e}")
                    continue
            rec["saved_as"] = dest.relative_to(DUMP).as_posix()
            manifest.append(rec)

        if manifest:                       # write/update manifest.json
            att_dir.mkdir(parents=True, exist_ok=True)
            with open(att_dir / "manifest.json", "w", encoding="utf-8") as fh:
                json.dump(manifest, fh, indent=2)

    return html, props, manifest, doc

# ── 3.  Attachment helpers ----------------------------------------------
re_src_href = re.compile(r'''(?:src|href)=["']([^"']+)["']''', re.I)

def slugify(text: str) -> str:
    """Make a filename-safe slug (preserve dots & dashes)."""
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", text).strip("_")

def find_attachment_links(html_body: str) -> list[dict]:
    """
    Return [{url, att_id, file_name}, …] for every attachment reference
    **inside the HTML itself**.  HTML entities are unescaped so &amp; → &.
    """
    links: list[dict] = []
    for raw in re_src_href.findall(html_body):
        url = html_lib.unescape(raw)           
        if "attachmentservice/attachments/getMedia" in url:
            file_name = url.split("/")[-1].split("?")[0]
            links.append({
                "url":       up.urljoin(BASE_URL, url),
                "att_id":    "media",
                "file_name": file_name,
            })
        elif "openAttachment.do" in url:
            qs = up.parse_qs(up.urlparse(url).query)
            if "att.id" in qs and "att.fileName" in qs:
                links.append({
                    "url":       up.urljoin(BASE_URL, url),
                    "att_id":    qs["att.id"][0],
                    "file_name": qs["att.fileName"][0],
                })
    return links

def download_attachment(rec: dict, dest: pathlib.Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".part")
    r = s.get(rec["url"], timeout=60, verify=CA_FILE if CA_FILE else True, stream=True)
    r.raise_for_status()
    with open(tmp, "wb") as fh:
        for chunk in r.iter_content(chunk_size=65536):
            fh.write(chunk)
    tmp.rename(dest)

def save_metadata(doc: dict, props: dict, out_path: pathlib.Path) -> None:
    meta = {
        "docid":          props.get("docid"),
        "guid":           props.get("guid"),
        "version":        props.get("version"),
        "title":          props.get("title"),
        "summary":        props.get("summary"),
        "publishdate":    props.get("publishdate"),
        "doctype":        props.get("doctypetitle"),
        "categoryPath":   (doc.get("categories") or {}).get("categoryBranchText"),
        "keywords":       doc.get("keywords", []),
        "kcUrl":          props.get("path"),
    }
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(meta, fh, ensure_ascii=False, indent=2)

# ── 4.  Main -------------------------------------------------------------
def main():
    docs = list(csv.DictReader(open("test.csv", newline="", encoding="utf-8")))
    print(f"{len(docs):,} documents listed in docs.csv")

    skipped, done = 0, 0
    for row in docs:
        guid     = row["guid"]
        # filename = DUMP / f"{guid}_{version}.html"
        # if filename.exists():
        #     skipped += 1
        #     continue

        # 4.1 Fetch HTML + props
        try:
            html_body, props, manifest, doc = fetch_html(guid, download_atts=DOWNLOAD_ATTACHMENTS)
        except Exception as e:
            print(f"✗ {guid} – {e}")
            continue

        version  = props.get("version", "0")           
        filename = DUMP / f"{guid}_{version}.html"

        if filename.exists():                          # skip if we already have this version
            skipped += 1
            continue

        # 4.2 Wrap & save HTML
        doc_title   = props.get("title", f"doc {guid}")
        doc_summary = props.get("summary", "")
        with open(filename, "w", encoding="utf-8") as fh:
            fh.write(f"<!-- guid:{guid} docid:{props.get('docid','?')} -->\n")
            fh.write("<html><head><meta charset='utf-8'>"
                     f"<title>{html.escape(doc_title)}</title></head>\n<body>\n")
            fh.write(f"<h1>{html.escape(doc_title)}</h1>\n")
            if doc_summary:
                fh.write(f"<p><em>{html.escape(doc_summary)}</em></p>\n")
            fh.write("<hr/>\n")
            fh.write(html_body)
            fh.write("\n</body></html>\n")

        meta_path = filename.with_suffix(".meta.json")
        save_metadata(doc, props, meta_path)

        done += 1
        print(f"✓ {guid} → {filename.name}")
        sleep(PAUSE)

    print(f"\nFinished: {done} downloaded, {skipped} already on disk.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit("\nInterrupted by user.")
