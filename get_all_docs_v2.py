#!/usr/bin/env python3
"""
get_all_docs.py – dump the KC index (all metadata) to docs.csv
"""

import csv, math, textwrap, requests, urllib.parse as up
import json, re, xmltodict

# ── 0.  Config -----------------------------------------------------------
APP_ID   = "3ea38aee-2df7-4cfe-a13d-a7161958bded"
PAGE     = 200
ENDPOINT = ("https://knowledge.revenue.nsw.gov.au/"
            "knowledgebase/services/DocumentService")
PROXY    = "http://127.0.0.1:3128"        # CNTLM relay
CA_FILE  = None

COOKIES = {                               # captured from browser session
    "JSESSIONID":  "C3AF4C40DC78103AD2990D00FDA6AA1B",
    "GKSESSIONID": "XUJDgiAz2tXPjX1v21fttgMrfGmjGwjx",
    "GKREALM":     "defaulthost",
    "GKCSRFTOKEN": "BjWRl2xE9jIE065v58ElBvXIo47mOLM3",
}

# ── 1.  Requests session -------------------------------------------------
s = requests.Session()
s.proxies  = {"http": PROXY, "https": PROXY}
s.cookies.update(COOKIES)

def csrf() -> str:
    for c in reversed(list(s.cookies)):
        if c.name == "GKCSRFTOKEN":
            return c.value
    raise RuntimeError("GKCSRFTOKEN cookie not found")

# ── 2.  Build SOAP envelope ---------------------------------------------
SOAP = textwrap.dedent("""\
    <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
                   xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                   xmlns:xsd="http://www.w3.org/2001/XMLSchema">
      <soap:Header></soap:Header>
      <soap:Body xmlns:ns0="http://ws.atlantis.usu.de/documentservice">
        <ns0:getDocumentsFromIndexReq>
          <appAreaId>{app}</appAreaId>
          <language>en_US</language>
          <contentLanguage>en</contentLanguage>
          <parameters>
            <paramType>isChangeRequest</paramType>
            <paramValue>false</paramValue>
            <paramOperator></paramOperator>
          </parameters>
          <parameters>
            <paramType>isOldVersion</paramType>
            <paramValue>false</paramValue>
            <paramOperator></paramOperator>
          </parameters>
          <parameters>
            <paramType>categoryBranch</paramType>
            <paramValue>8_24_13_*</paramValue>
            <paramOperator>equal</paramOperator>
          </parameters>
          <orderBy>
            <fieldKeyType>standard</fieldKeyType>
            <fieldKey>fullVersion</fieldKey>
            <direction>desc</direction>
          </orderBy>
          <withContent>false</withContent>
          <withMetaData>false</withMetaData>
          <additionalMeta>
            <paramName>DocumentListMainData</paramName>
            <paramValue>true</paramValue>
            <paramOperator></paramOperator>
          </additionalMeta>
          <additionalMeta>
            <paramName>extraProperties</paramName>
            <paramValue>creatordate</paramValue>
            <paramOperator></paramOperator>
          </additionalMeta>
          <startIndex>{start}</startIndex>
          <maxCount>{count}</maxCount>
          <sessionId></sessionId>
        </ns0:getDocumentsFromIndexReq>
      </soap:Body>
    </soap:Envelope>""")

# ── 3.  Helpers ----------------------------------------------------------
def extract_json_from_text(text: str) -> dict:
    m = re.search(r'\{.*\}\s*$', text, flags=re.S)
    if not m:
        raise ValueError("no JSON block in response")
    return json.loads(m.group(0))

def get_page(start: int) -> dict:
    body = SOAP.format(app=APP_ID, start=start, count=PAGE)
    r = s.post(
        ENDPOINT,
        data=body.encode(),
        headers={
            "Content-Type":  "text/xml; charset=UTF-8",
            "Accept":        "application/json",
            "GK-CSRF-Token": csrf(),
        },
        timeout=45,
        verify=CA_FILE if CA_FILE else True,
    )
    r.raise_for_status()

    try:                                    # JSON straight away?
        return r.json()["Envelope"]["Body"]["getDocumentsFromIndexRes"]
    except (ValueError, KeyError):
        pass

    envelope  = xmltodict.parse(r.text)
    body_node = envelope["soap:Envelope"]["soap:Body"]

    if isinstance(body_node, str):         # JSON string inside <Body>
        data = json.loads(body_node)
        return data["Envelope"]["Body"]["getDocumentsFromIndexRes"]

    if isinstance(body_node, dict):        # XML node inside <Body>
        for k, v in body_node.items():
            if k.endswith('getDocumentsFromIndexRes'):
                return v

    data = extract_json_from_text(r.text)  # fallback: JSON after </Envelope>
    return data["Envelope"]["Body"]["getDocumentsFromIndexRes"]

# ── 4.  Fetch everything, union property names --------------------------
first   = get_page(0)
total   = int(first["totalCount"])
pages   = math.ceil(total / PAGE)
print(f"Server reports {total:,} documents → {pages} pages of {PAGE}")

rows, all_keys = [], set()

def collect(doc: dict):
    props = {p["name"]: p["value"] for p in doc["properties"]}
    rows.append(props)
    all_keys.update(props.keys())

for d in first["documents"]:
    collect(d)

for p in range(1, pages):
    resp = get_page(p * PAGE)
    for d in resp["documents"]:
        collect(d)
    print(f"page {p+1}/{pages} done")

# ── 5.  Write CSV --------------------------------------------------------
fieldnames = sorted(all_keys)

with open("docs.csv", "w", newline="", encoding="utf-8") as fh:
    wr = csv.writer(fh)
    wr.writerow(fieldnames)                # header

    for props in rows:
        wr.writerow([props.get(k, "") for k in fieldnames])

print("Done → docs.csv")
