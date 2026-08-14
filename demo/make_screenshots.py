"""Render terminal-style PNG 'screenshots' from real captured pipeline output."""
from PIL import Image, ImageDraw, ImageFont
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FONT_PATH = "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"
FONT_SIZE = 15
PAD = 24
LINE_H = 20
TITLEBAR_H = 34
BG = (25, 27, 33)
TITLEBAR_BG = (43, 45, 53)
TEXT_COLOR = (223, 227, 232)
GREEN = (98, 209, 150)
CYAN = (95, 195, 234)
YELLOW = (229, 192, 123)
DOT_COLORS = [(255, 95, 86), (255, 189, 46), (39, 201, 63)]


def render(lines, title, out_path, width=1180):
    font = ImageFont.truetype(FONT_PATH, FONT_SIZE)
    height = TITLEBAR_H + PAD * 2 + LINE_H * len(lines)
    img = Image.new("RGB", (width, height), BG)
    draw = ImageDraw.Draw(img)

    draw.rectangle([0, 0, width, TITLEBAR_H], fill=TITLEBAR_BG)
    for i, c in enumerate(DOT_COLORS):
        draw.ellipse([16 + i * 22, 11, 16 + i * 22 + 12, 23], fill=c)
    draw.text((width / 2 - len(title) * 3.6, 8), title, font=font, fill=(180, 184, 191))

    y = TITLEBAR_H + PAD
    for line in lines:
        color = TEXT_COLOR
        if line.strip().startswith(">>>") or line.strip().startswith("==="):
            color = CYAN
        elif "true" in line and "|" in line:
            color = TEXT_COLOR
        elif line.strip().startswith("+--") or line.strip().startswith("|"):
            color = GREEN
        elif "root" in line or line.strip().startswith("|--"):
            color = YELLOW
        draw.text((PAD, y), line, font=font, fill=color)
        y += LINE_H

    img.save(out_path)
    print("Saved", out_path)


def load(path, start, end):
    with open(path) as f:
        all_lines = f.read().split("\n")
    return all_lines[start:end]


if __name__ == "__main__":
    log = os.path.join(BASE_DIR, "output", "run_log_clean.txt")
    shots = os.path.join(BASE_DIR, "screenshots")

    with open(log) as f:
        all_lines = f.read().split("\n")

    def find(marker):
        for i, l in enumerate(all_lines):
            if marker in l:
                return i
        return -1

    s1 = find("STAGE 1: READ FROM WEB API SOURCE")
    render(all_lines[s1:s1 + 30], "python3 demo/pipeline_demo.py — Stage 1: Web API JSON ingestion",
           os.path.join(shots, "01_stage1_webapi_ingest.png"))

    s4 = find("STAGE 4: STITCH")
    render(all_lines[s4:s4 + 24], "python3 demo/pipeline_demo.py — Stage 4: Stitching HDFS onto Web API data",
           os.path.join(shots, "02_stage4_stitch_join.png"))

    s6 = find("SELECT * FROM analytics.customer_360_curated")
    render(all_lines[s6:s6 + 16], "pyspark-sql — Final curated Hive table",
           os.path.join(shots, "03_hive_table_output.png"))

    s7 = find("ANALYTICS PREVIEW")
    end = find("Final curated row count")
    render(all_lines[s7:end + 1], "pyspark-sql — High-value / high-engagement customer query",
           os.path.join(shots, "04_analytics_query_output.png"))
