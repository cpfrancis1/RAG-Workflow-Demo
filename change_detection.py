"""
change_detection.py – keep docs.csv in sync with the USU KC index
• Pulls a fresh index   (same SOAP as get_all_docs_v2.py)
• Compares to docs.csv  (must exist in the working dir)
• Outputs:
      docs.csv           ← updated master list
      new_docs.csv       ← brand-new docs
      updated_docs.csv   ← same GUID, higher version
      deleted_docs.csv   ← GUIDs no longer present / unpublished
"""

import csv, math, textwrap, requests, urllib.parse as up, json, re, xmltodict
from pathlib import Path

# ── 0.  Config (keep in-sync with your other scripts) ───────────────────
APP_ID   = "3ea38aee-2df7-4cfe-a13d-a7161958bded"
PAGE     = 200                        # index page size
ENDPOINT = ("https://knowledge.revenue.nsw.gov.au/"
            "knowledgebase/services/DocumentService")
PROXY    = "http://127.0.0.1:3128"    # CNTLM relay
CA_FILE  = None                       # or your corp CA bundle

COOKIES  = {                          # captured from browser session
    "JSESSIONID":  "D25E52C50B09F1A8A520563F7A55D233",
    "GKSESSIONID": "L9puLeb16QPjOu1RmCrIysYEg7Pq8MYb",
    "GKREALM":     "defaulthost",
    "GKCSRFTOKEN": "k1HFikT02MbvJh0ScC8V9KoBb60E9otH",
}

DOCS_CSV = Path("docs.csv")           # master list (must exist)

# ── 1.  Session & helpers ───────────────────────────────────────────────
s = requests.Session()
s.proxies  = {"http": PROXY, "https": PROXY}
s.cookies.update(COOKIES)

def csrf() -> str:
    for c in reversed(list(s.cookies)):
        if c.name.upper() == "GKCSRFTOKEN":
            return c.value
    raise RuntimeError("GKCSRFTOKEN cookie missing")

SOAP = textwrap.dedent("""\
    <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
                   xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                   xmlns:xsd="http://www.w3.org/2001/XMLSchema">
      <soap:Body xmlns:ns0="http://ws.atlantis.usu.de/documentservice">
        <ns0:getDocumentsFromIndexReq>
          <appAreaId>{app}</appAreaId>
          <language>en_US</language>
          <contentLanguage>en</contentLanguage>
          <parameters><paramType>isChangeRequest</paramType><paramValue>false</paramValue></parameters>
          <parameters><paramType>isOldVersion</paramType><paramValue>false</paramValue></parameters>
          <!-- duties branch only (tweak as needed) -->
          <parameters><paramType>categoryBranch</paramType><paramValue>8_24_13_*</paramValue><paramOperator>equal</paramOperator></parameters>
          <orderBy><fieldKeyType>standard</fieldKeyType><fieldKey>fullVersion</fieldKey><direction>desc</direction></orderBy>
          <withContent>false</withContent><withMetaData>false</withMetaData>
          <additionalMeta><paramName>DocumentListMainData</paramName><paramValue>true</paramValue></additionalMeta>
          <startIndex>{start}</startIndex><maxCount>{count}</maxCount><sessionId/>
        </ns0:getDocumentsFromIndexReq>
      </soap:Body>
    </soap:Envelope>""")

def get_page(start: int) -> dict:
    body = SOAP.format(app=APP_ID, start=start, count=PAGE)
    r = s.post(
        ENDPOINT, data=body.encode(),
        headers={"Content-Type":"text/xml; charset=UTF-8",
                 "Accept":"application/json",
                 "GK-CSRF-Token":csrf()},
        timeout=45, verify=CA_FILE or False)
    r.raise_for_status()
    try:
        return r.json()["Envelope"]["Body"]["getDocumentsFromIndexRes"]
    except (ValueError, KeyError):
        # fallbacks for weird XML/JSON hybrids
        data = xmltodict.parse(r.text)
        body = data["soap:Envelope"]["soap:Body"]
        for k,v in body.items():
            if k.endswith("getDocumentsFromIndexRes"):
                return v
        raise RuntimeError("unexpected payload structure")

def fetch_full_index() -> list[dict]:
    first = get_page(0)
    total = int(first["totalCount"])
    pages = math.ceil(total / PAGE)
    docs  = list(first["documents"])
    for p in range(1, pages):
        docs.extend(get_page(p*PAGE)["documents"])
    # flatten properties into a simple dict for each doc
    rows = []
    for d in docs:
        props = {p["name"].lower(): p["value"] for p in d["properties"]}
        rows.append(props)
    return rows

# ── 2.  Diff against existing docs.csv ──────────────────────────────────
def csv_to_dict(path: Path) -> dict[str,dict]:
    rows = {}
    with open(path, newline="", encoding="utf-8") as fh:
        rdr = csv.DictReader(fh)
        for row in rdr:
            rows[row["guid"]] = row
    return rows, rdr.fieldnames           # keep original column order

print("Fetching latest index …")
latest_rows = fetch_full_index()
latest_by_guid = {r["guid"]: r for r in latest_rows}

print(f"Index contains {len(latest_by_guid):,} docs")

if not DOCS_CSV.exists():
    raise SystemExit("✗ docs.csv missing – run get_all_docs_v2.py first.")

old_rows, fieldnames = csv_to_dict(DOCS_CSV)
print(f"Existing docs.csv has {len(old_rows):,} docs")

# sets for quick membership tests
old_guids = set(old_rows)
new_guids = set(latest_by_guid)

added    = new_guids - old_guids
deleted  = old_guids - new_guids
common   = old_guids & new_guids

updated  = {g for g in common
            if latest_by_guid[g]["fullversion"] != old_rows[g]["fullversion"]}

print(f"➕  {len(added):>4} new")
print(f"🔄  {len(updated):>4} updated version")
print(f"➖  {len(deleted):>4} removed/unpublished")

# ── 3.  Write helper CSVs (for downstream processes) ─────────────────────
def write_subset(filename: str, guids: set[str]):
    if not guids:
        Path(filename).unlink(missing_ok=True)
        return
    with open(filename, "w", newline="", encoding="utf-8") as fh:
        wr = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        wr.writeheader()
        for g in sorted(guids):
            wr.writerow(latest_by_guid.get(g) or old_rows.get(g))

write_subset("new_docs.csv",      added)
write_subset("updated_docs.csv",  updated)
write_subset("deleted_docs.csv",  deleted)

# ── 4.  Merge and overwrite docs.csv ─────────────────────────────────────
merged = {**old_rows, **{g: latest_by_guid[g] for g in added|updated}}
for g in deleted:
    merged.pop(g, None)

with open(DOCS_CSV, "w", newline="", encoding="utf-8") as fh:
    wr = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
    wr.writeheader()
    for row in merged.values():
        wr.writerow(row)

print("✓ docs.csv updated")