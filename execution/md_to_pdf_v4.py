#!/usr/bin/env python3
import re
import os
import sys
from pathlib import Path
from fpdf import FPDF

class InvestigationPDF(FPDF):
    def __init__(self):
        super().__init__()
        # Load fonts
        font_dir = "/System/Library/Fonts/Supplemental"
        if os.path.exists(f"{font_dir}/Arial.ttf"):
            self.add_font("Arial", "", f"{font_dir}/Arial.ttf")
            self.add_font("Arial", "B", f"{font_dir}/Arial Bold.ttf")
            self.add_font("Arial", "I", f"{font_dir}/Arial Italic.ttf")
            self.default_font = "Arial"
        else:
            self.default_font = "Helvetica"
            
        self.set_auto_page_break(auto=True, margin=15)
        self.add_page()
        self.set_font(self.default_font, "", 10)

    def header(self):
        self.set_font(self.default_font, "B", 8)
        self.set_text_color(180, 0, 0)
        self.cell(0, 10, "RESTRICTED // LAW ENFORCEMENT SENSITIVE // FININT", 0, 0, "C")
        self.ln(10)

    def footer(self):
        self.set_y(-15)
        self.set_font(self.default_font, "I", 8)
        self.set_text_color(128)
        self.cell(0, 10, f"Page {self.page_no()} | Operation CRYSTAL SABOT | Strategic OSINT Unit", 0, 0, "C")

def convert_md_to_pdf(input_md, output_pdf):
    pdf = InvestigationPDF()
    
    with open(input_md, "r", encoding="utf-8") as f:
        content = f.read()

    # Split into lines
    lines = content.split("\n")
    
    in_list = False
    
    for line in lines:
        line = line.strip()
        
        # Images: ![alt](path)
        img_match = re.search(r"!\[(.*?)\]\((.*?)\)", line)
        if img_match:
            img_path = img_match.group(2).replace("file://", "")
            if os.path.exists(img_path):
                try:
                    pdf.ln(5)
                    # Calculate width to fit page
                    pdf.image(img_path, w=pdf.epw * 0.8, x=(pdf.w - pdf.epw * 0.8)/2)
                    pdf.set_font(pdf.default_font, "I", 8)
                    pdf.cell(0, 5, f"Figure: {img_match.group(1)}", 0, 1, "C")
                    pdf.ln(5)
                except Exception as e:
                    print(f"Error embedding image {img_path}: {e}")
            continue

        # Headers
        if line.startswith("# "):
            pdf.set_font(pdf.default_font, "B", 20)
            pdf.set_text_color(0, 0, 128)
            pdf.ln(10)
            pdf.multi_cell(0, 10, line[2:], align="C")
            pdf.ln(5)
        elif line.startswith("## "):
            pdf.set_font(pdf.default_font, "B", 16)
            pdf.set_text_color(0, 50, 150)
            pdf.ln(8)
            pdf.multi_cell(0, 8, line[3:])
            pdf.ln(2)
        elif line.startswith("### "):
            pdf.set_font(pdf.default_font, "B", 13)
            pdf.set_text_color(0, 100, 0)
            pdf.ln(5)
            pdf.multi_cell(0, 7, line[4:])
            pdf.ln(1)
        
        # Tables (Simplified)
        elif line.startswith("|"):
            if "---" in line: continue
            cells = [c.strip() for c in line.split("|") if c.strip()]
            if cells:
                pdf.set_font(pdf.default_font, "B" if "Detail" in line or "Name" in line or "Role" in line else "", 9)
                pdf.set_text_color(0)
                col_width = pdf.epw / len(cells)
                for i, cell in enumerate(cells):
                    pdf.multi_cell(col_width, 6, cell, border=1, ln=3 if i < len(cells)-1 else 1)
                pdf.ln(2)
        
        # Horizontal Rule
        elif line == "---" or line == "***":
            pdf.ln(2)
            pdf.line(pdf.l_margin, pdf.get_y(), pdf.w - pdf.r_margin, pdf.get_y())
            pdf.ln(5)
            
        # Lists
        elif line.startswith("- ") or line.startswith("* "):
            pdf.set_font(pdf.default_font, "", 10)
            pdf.set_text_color(0)
            pdf.set_x(pdf.l_margin + 5)
            pdf.multi_cell(0, 5, f"• {line[2:]}")
            pdf.set_x(pdf.l_margin)
        
        # Blockquotes
        elif line.startswith("> "):
            pdf.set_font(pdf.default_font, "I", 10)
            pdf.set_text_color(100)
            pdf.set_x(pdf.l_margin + 10)
            text = line[2:]
            if "[!IMPORTANT]" in text or "[!CAUTION]" in text:
                pdf.set_font(pdf.default_font, "B", 10)
                pdf.set_text_color(180, 0, 0)
                text = text.replace("[!IMPORTANT]", "IMPORTANT:").replace("[!CAUTION]", "CAUTION:")
            pdf.multi_cell(pdf.epw - 10, 5, text, border="L")
            pdf.set_x(pdf.l_margin)
            pdf.ln(2)

        # Standard Text
        elif line:
            # Clean bold/italic markdown
            text = re.sub(r"\*\*([^*]+)\*\*", r"\1", line)
            text = re.sub(r"\*([^*]+)\*", r"\1", text)
            
            pdf.set_font(pdf.default_font, "", 10)
            pdf.set_text_color(0)
            pdf.set_x(pdf.l_margin)
            pdf.multi_cell(0, 5, text)
        else:
            pdf.ln(2)

    pdf.output(output_pdf)
    print(f"Successfully generated PDF: {output_pdf}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 md_to_pdf_v4.py <input.md> [output.pdf]")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else input_file.replace(".md", ".pdf")
    convert_md_to_pdf(input_file, output_file)
