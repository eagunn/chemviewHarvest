"""
Generate statistics for the chemview_archive_8e archive.

- Walk immediate subfolders of `chemview_archive_8e` that look like `CAS-...`.
- For each CAS folder, count HTML and PDF files and sum their sizes.
- Produce totals and averages and project storage for 14,690 CAS entries.
- Write a text report file (default: status_8e_report.txt).

Usage:
    python status_8e.py [--root PATH] [--output PATH] [--project-cas N]

Defaults assume this script runs from the `harvest` folder and the
`chemview_archive_8e` folder is next to it.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Dict, Tuple

DEFAULT_ROOT = Path("chemview_archive_8e")
DEFAULT_OUTPUT = Path("status_8e_report.txt")
DEFAULT_PROJECT_CAS = 14690


def format_size(nbytes: int) -> str:
    """Return human friendly file size, capped at GB (no TB output).

    Examples: "123 bytes", "1.23 MB", "2.34 GB"
    """
    # prefer units up to GB
    for unit in ("bytes", "KB", "MB", "GB"):
        if nbytes < 1024.0 or unit == "GB":
            if unit == "bytes":
                return f"{nbytes:,} {unit}"
            # compute divisor
            unit_index = ("bytes", "KB", "MB", "GB").index(unit)
            return f"{nbytes/1024.0**unit_index:.2f} {unit}"
    return f"{nbytes:,} bytes"


def scan_cas_folder(cas_path: Path) -> Tuple[int, int, int, int, int, int]:
    """Scan a CAS folder recursively.

    Returns: (html_count, html_bytes, pdf_count, pdf_bytes, xml_count, xml_bytes)
    """
    html_count = 0
    html_bytes = 0
    pdf_count = 0
    pdf_bytes = 0
    xml_count = 0
    xml_bytes = 0

    for root, _, files in os.walk(cas_path):
        for fn in files:
            lower = fn.lower()
            try:
                full = Path(root) / fn
                size = full.stat().st_size
            except OSError:
                # skip unreadable files
                continue
            if lower.endswith('.html') or lower.endswith('.htm'):
                html_count += 1
                html_bytes += size
            elif lower.endswith('.pdf'):
                pdf_count += 1
                pdf_bytes += size
            elif lower.endswith('.xml'):
                xml_count += 1
                xml_bytes += size

    return html_count, html_bytes, pdf_count, pdf_bytes, xml_count, xml_bytes


def main():
    parser = argparse.ArgumentParser(description="Produce storage statistics for chemview_archive_8e")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT, help="Archive root directory (default: chemview_archive_8e)")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Output report file")
    parser.add_argument("--project-cas", type=int, default=DEFAULT_PROJECT_CAS, help="Number of CAS to project for (default: 14690)")
    args = parser.parse_args()

    root: Path = args.root
    output: Path = args.output
    project_cas: int = args.project_cas

    if not root.exists() or not root.is_dir():
        raise SystemExit(f"Archive root not found or not a directory: {root}")

    # Find immediate CAS-* subdirectories
    cas_dirs = [p for p in sorted(root.iterdir()) if p.is_dir() and p.name.startswith('CAS-')]
    total_cas = len(cas_dirs)

    per_cas_stats: Dict[str, Dict[str, int]] = {}

    total_html_files = 0
    total_html_bytes = 0
    total_pdf_files = 0
    total_pdf_bytes = 0
    total_xml_files = 0
    total_xml_bytes = 0

    for cas in cas_dirs:
        html_count, html_bytes, pdf_count, pdf_bytes, xml_count, xml_bytes = scan_cas_folder(cas)
        per_cas_stats[cas.name] = {
            'html_count': html_count,
            'html_bytes': html_bytes,
            'pdf_count': pdf_count,
            'pdf_bytes': pdf_bytes,
            'xml_count': xml_count,
            'xml_bytes': xml_bytes,
        }
        total_html_files += html_count
        total_html_bytes += html_bytes
        total_pdf_files += pdf_count
        total_pdf_bytes += pdf_bytes
        total_xml_files += xml_count
        total_xml_bytes += xml_bytes

    # CAS with at least one HTML/PDF/XML
    cas_with_html = sum(1 for v in per_cas_stats.values() if v['html_count'] > 0)
    cas_with_pdf = sum(1 for v in per_cas_stats.values() if v['pdf_count'] > 0)
    cas_with_xml = sum(1 for v in per_cas_stats.values() if v['xml_count'] > 0)
    cas_no_files = sum(1 for v in per_cas_stats.values() if (v['html_count'] + v['pdf_count'] + v['xml_count']) == 0)

    # Averages per CAS (including zeros)
    avg_html_bytes_per_cas = total_html_bytes / total_cas if total_cas else 0
    avg_pdf_bytes_per_cas = total_pdf_bytes / total_cas if total_cas else 0
    avg_xml_bytes_per_cas = total_xml_bytes / total_cas if total_cas else 0

    # Averages per active CAS (only those with at least one file)
    avg_html_bytes_per_active_cas = (total_html_bytes / cas_with_html) if cas_with_html else 0
    avg_pdf_bytes_per_active_cas = (total_pdf_bytes / cas_with_pdf) if cas_with_pdf else 0
    avg_xml_bytes_per_active_cas = (total_xml_bytes / cas_with_xml) if cas_with_xml else 0

    # Projections
    # Use averages calculated over active CAS (those with at least one file)
    projected_total_html_bytes = int(avg_html_bytes_per_active_cas * project_cas)
    projected_total_pdf_bytes = int(avg_pdf_bytes_per_active_cas * project_cas)
    projected_total_xml_bytes = int(avg_xml_bytes_per_active_cas * project_cas)
    # combined projection
    projected_total_all_bytes = projected_total_html_bytes + projected_total_pdf_bytes + projected_total_xml_bytes

    # Write report
    lines = []
    lines.append("Status report for chemview_archive_8e")
    lines.append("Root: " + str(root))
    lines.append("")
    lines.append(f"Total CAS folders found: {total_cas:,}")
    # active CAS are those with at least one file (html/pdf/xml)
    active_total = total_cas - cas_no_files
    pct_active = (active_total / total_cas * 100) if total_cas else 0.0
    lines.append(f"Active CAS folders: {active_total:,} ({pct_active:.2f}%)")
    lines.append("")
    lines.append("HTML files:")
    lines.append(f"  Total HTML files: {total_html_files:,}")
    lines.append(f"  Total HTML bytes: {total_html_bytes:,} ({format_size(total_html_bytes)})")
    lines.append(f"  CAS with HTML files: {cas_with_html:,}")
    lines.append(f"  Average HTML bytes per CAS (including zeros): {avg_html_bytes_per_cas:,.2f} ({format_size(int(avg_html_bytes_per_cas))})")
    lines.append(f"  Average HTML bytes per CAS (active CAS only): {avg_html_bytes_per_active_cas:,.2f} ({format_size(int(avg_html_bytes_per_active_cas))})")
    lines.append("")
    lines.append("PDF files:")
    lines.append(f"  Total PDF files: {total_pdf_files:,}")
    lines.append(f"  Total PDF bytes: {total_pdf_bytes:,} ({format_size(total_pdf_bytes)})")
    lines.append(f"  CAS with PDF files: {cas_with_pdf:,}")
    lines.append(f"  Average PDF bytes per CAS (including zeros): {avg_pdf_bytes_per_cas:,.2f} ({format_size(int(avg_pdf_bytes_per_cas))})")
    lines.append(f"  Average PDF bytes per CAS (active CAS only): {avg_pdf_bytes_per_active_cas:,.2f} ({format_size(int(avg_pdf_bytes_per_active_cas))})")
    lines.append("")
    lines.append("XML files:")
    lines.append(f"  Total XML files: {total_xml_files:,}")
    lines.append(f"  Total XML bytes: {total_xml_bytes:,} ({format_size(total_xml_bytes)})")
    lines.append(f"  CAS with XML files: {cas_with_xml:,}")
    lines.append(f"  Average XML bytes per CAS (including zeros): {avg_xml_bytes_per_cas:,.2f} ({format_size(int(avg_xml_bytes_per_cas))})")
    lines.append(f"  Average XML bytes per CAS (active CAS only): {avg_xml_bytes_per_active_cas:,.2f} ({format_size(int(avg_xml_bytes_per_active_cas))})")
    lines.append("")
    lines.append(f"CAS folders with no files so far: {cas_no_files:,}")
    lines.append("")
    lines.append(f"Projection for {project_cas:,} CAS entries (using averages across active CAS — those with at least one file):")
    lines.append(f"  Projected total HTML bytes: {projected_total_html_bytes:,} ({format_size(projected_total_html_bytes)})")
    lines.append(f"  Projected total PDF bytes: {projected_total_pdf_bytes:,} ({format_size(projected_total_pdf_bytes)})")
    lines.append(f"  Projected total XML bytes: {projected_total_xml_bytes:,} ({format_size(projected_total_xml_bytes)})")
    lines.append(f"  Projected total (all types) bytes: {projected_total_all_bytes:,} ({format_size(projected_total_all_bytes)})")
    lines.append("")
    lines.append("Per-CAS summary (first 50 entries shown):")
    shown = 0
    for cas_name, stats in per_cas_stats.items():
        if shown >= 50:
            break
        lines.append(f"  {cas_name}: html_files={stats['html_count']}, html_bytes={stats['html_bytes']:,} ({format_size(stats['html_bytes'])}), pdf_files={stats['pdf_count']}, pdf_bytes={stats['pdf_bytes']:,} ({format_size(stats['pdf_bytes'])}), xml_files={stats['xml_count']}, xml_bytes={stats['xml_bytes']:,} ({format_size(stats['xml_bytes'])})")
        shown += 1

    lines.append("")
    lines.append("End of report")

    output_text = "\n".join(lines)
    with open(output, 'w', encoding='utf-8') as fh:
        fh.write(output_text)

    print(f"Report written to {output}")


if __name__ == '__main__':
    main()
