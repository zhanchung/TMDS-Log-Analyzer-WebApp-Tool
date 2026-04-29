from __future__ import annotations

import argparse
import json
import re
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET


PRESENTATION_NS = {
    "a": "http://schemas.openxmlformats.org/drawingml/2006/main",
}


def normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def slide_sort_key(path: str) -> tuple[int, str]:
    match = re.search(r"slide(\d+)\.xml$", path)
    return (int(match.group(1)) if match else 0, path)


def extract_slide_texts(pptx_path: Path) -> list[dict]:
    slides: list[dict] = []
    with zipfile.ZipFile(pptx_path) as archive:
        slide_names = sorted(
            [name for name in archive.namelist() if name.startswith("ppt/slides/slide") and name.endswith(".xml")],
            key=slide_sort_key,
        )

        for slide_name in slide_names:
            xml_bytes = archive.read(slide_name)
            root = ET.fromstring(xml_bytes)
            texts = [normalize_whitespace(node.text or "") for node in root.findall(".//a:t", PRESENTATION_NS)]
            texts = [text for text in texts if text]
            slides.append(
                {
                    "slide_name": slide_name,
                    "slide_number": slide_sort_key(slide_name)[0],
                    "text_items": texts,
                    "text": "\n".join(texts),
                }
            )
    return slides


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract slide text from a PPTX file.")
    parser.add_argument("pptx_path", type=Path)
    parser.add_argument("--text-out", type=Path, required=True)
    parser.add_argument("--json-out", type=Path, required=True)
    args = parser.parse_args()

    slides = extract_slide_texts(args.pptx_path)
    args.text_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.parent.mkdir(parents=True, exist_ok=True)

    text_lines: list[str] = []
    for slide in slides:
        text_lines.append(f"=== Slide {slide['slide_number']} ===")
        if slide["text"]:
            text_lines.append(slide["text"])
        text_lines.append("")

    args.text_out.write_text("\n".join(text_lines), encoding="utf-8")
    args.json_out.write_text(json.dumps(slides, indent=2, ensure_ascii=False), encoding="utf-8")

    print(
        json.dumps(
            {
                "pptx": str(args.pptx_path),
                "slide_count": len(slides),
                "text_out": str(args.text_out),
                "json_out": str(args.json_out),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
