#!/usr/bin/env python3
"""
Convert every *.html under dump/ to clean Markdown (*.md).

Usage:
    python html_to_md.py   # processes dump/**/*<guid>.html

Dependencies:
    pip install beautifulsoup4 markdownify
"""
import pathlib, re, sys, html
from bs4 import BeautifulSoup
from markdownify import markdownify as md

# ------- config ----------------------------------------------------------
ROOT = pathlib.Path("dump")            # where your HTML files live
GLOB = "**/*.html"                     
CLEAN_TAGS_RE = re.compile(r"^(script|style|nav)$", re.I)
# -------------------------------------------------------------------------

def html_to_markdown(src: pathlib.Path, dst: pathlib.Path) -> None:
    text = src.read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(text, "lxml")

    # 1. Drop scripts, styles, nav bars, empty spans, etc.
    for tag in soup.find_all(CLEAN_TAGS_RE):
        tag.decompose()

    # 2. Unwrap span/div that only add styling
    for tag in soup.find_all(["span", "div"]):
        if not tag.attrs or set(tag.attrs).issubset({"style"}):
            tag.unwrap()

    # 3. Convert → Markdown (tables=True keeps <table>)
    md_txt = md(str(soup.body or soup), heading_style="ATX", strip=["img"], bullets="-")

    # 4. Normalise whitespace
    md_txt = re.sub(r"\n{3,}", "\n\n", md_txt).strip() + "\n"

    dst.write_text(md_txt, encoding="utf-8")
    print(f"✓ {dst.relative_to(ROOT)}")

def main():
    html_files = list(ROOT.glob(GLOB))
    if not html_files:
        sys.exit("No HTML files found – run download_docs.py first.")

    for src in html_files:
        dst = src.with_suffix(".md")
        html_to_markdown(src, dst)

if __name__ == "__main__":
    main()