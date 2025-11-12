# Centralized FileTypes used across harvest modules
# Keep this as the single authoritative source for file type names
# so all harvest scripts and the DB use the same values.

class FileTypes:
    section5_html = "section5_html"
    section5_pdf = "section5_pdf"
    substantial_risk_html = "substantial_risk_html"
    substantial_risk_pdf = "substantial_risk_pdf"
    new_chemical_notice_html = "new_chemical_notice_html"
    new_chemical_notice_pdf = "new_chemical_notice_pdf"

__all__ = ["FileTypes"]

