#!/usr/bin/env python3
"""
Markdown to PDF Converter using fpdf2
Simple converter - handles Unicode by using simpler text
"""

import sys
import argparse
import re
import unicodedata
from pathlib import Path
from fpdf import FPDF

class MarkdownPDF(FPDF):
    def __init__(self):
        super().__init__()
        self.add_page()
        self.set_auto_page_break(auto=True, margin=15)
        
    def header(self):
        self.set_font('helvetica', 'B', 8)
        self.set_text_color(128)
        self.cell(0, 5, 'Classification: INVESTIGATION SENSITIVE - OSINT', 0, 1, 'R')
        self.ln(3)
        
    def footer(self):
        self.set_y(-12)
        self.set_font('helvetica', 'I', 8)
        self.set_text_color(128)
        self.cell(0, 8, f'Page {self.page_no()}', 0, 0, 'C')


def clean_text(text):
    """Clean text for PDF - remove/replace problematic characters"""
    # Replace em-dashes
    text = text.replace('\u2014', '-')
    text = text.replace('\u2013', '-')
    # Replace quotes
    text = text.replace('\u201c', '"').replace('\u201d', '"')
    text = text.replace('\u2018', "'").replace('\u2019', "'")
    # Remove other problematic chars
    text = unicodedata.normalize('NFKD', text)
    return text


def parse_markdown_to_pdf(md_file, output_file=None):
    """Convert markdown to PDF"""
    
    md_path = Path(md_file)
    
    if not md_path.exists():
        print(f"Error: File {md_file} not found")
        return False
    
    # Read markdown content
    with open(md_path, 'r', encoding='utf-8') as f:
        md_content = f.read()
    
    # Determine output path
    if output_file:
        output_path = Path(output_file)
    else:
        output_path = md_path.parent / f"{md_path.stem}.pdf"
    
    # Create PDF
    pdf = MarkdownPDF()
    
    # Process markdown
    lines = md_content.split('\n')
    in_table = False
    
    for line in lines:
        line = line.strip()
        
        if not line:
            pdf.ln(3)
            continue
        
        # Title (first #)
        if line.startswith('# ') and not line.startswith('##'):
            pdf.set_font('helvetica', 'B', 18)
            pdf.set_text_color(0, 0, 0)
            pdf.ln(10)
            pdf.cell(0, 10, clean_text(line[2:]), 0, 1, 'C')
            pdf.ln(5)
        
        # Heading 1
        elif line.startswith('## '):
            pdf.set_font('helvetica', 'B', 14)
            pdf.set_text_color(0, 0, 0)
            pdf.ln(8)
            pdf.cell(0, 8, clean_text(line[3:]), 0, 1, 'L')
            pdf.ln(3)
        
        # Heading 2
        elif line.startswith('### '):
            pdf.set_font('helvetica', 'B', 12)
            pdf.set_text_color(0, 0, 0)
            pdf.ln(6)
            pdf.cell(0, 7, clean_text(line[4:]), 0, 1, 'L')
            pdf.ln(2)
        
        # Heading 3
        elif line.startswith('#### '):
            pdf.set_font('helvetica', 'B', 11)
            pdf.set_text_color(50, 50, 50)
            pdf.ln(5)
            pdf.cell(0, 6, clean_text(line[5:]), 0, 1, 'L')
        
        # Table
        elif line.startswith('|'):
            if '---' in line or line.startswith('|---'):
                continue
            cells = [clean_text(c.strip()) for c in line.split('|') if c.strip()]
            if cells:
                pdf.set_font('helvetica', '', 8)
                pdf.set_text_color(0, 0, 0)
                row_text = ' | '.join(cells[:4])  # Limit columns
                pdf.multi_cell(0, 5, row_text)
        
        # Bullet list
        elif line.startswith('- ') or line.startswith('* '):
            pdf.set_font('helvetica', '', 9)
            pdf.set_text_color(0, 0, 0)
            pdf.cell(8)
            pdf.multi_cell(0, 5, '• ' + clean_text(line[2:]))
        
        # Numbered list
        elif re.match(r'^\d+\. ', line):
            match = re.match(r'^(\d+)\. (.*)', line)
            if match:
                pdf.set_font('helvetica', '', 9)
                pdf.set_text_color(0, 0, 0)
                pdf.cell(8)
                pdf.multi_cell(0, 5, match.group(1) + '. ' + clean_text(match.group(2)))
        
        # Blockquote
        elif line.startswith('> '):
            pdf.set_font('helvetica', 'I', 9)
            pdf.set_text_color(100, 100, 100)
            pdf.multi_cell(0, 5, clean_text(line[2:]))
        
        # Regular text
        else:
            text = clean_text(line)
            text = re.sub(r'\*\*([^*]+)\*\*', r'\1', text)
            text = re.sub(r'\*([^*]+)\*', r'\1', text)
            text = re.sub(r'`([^`]+)`', r'\1', text)
            
            pdf.set_font('helvetica', '', 9)
            pdf.set_text_color(0, 0, 0)
            pdf.multi_cell(0, 4, text)
    
    # Save PDF
    try:
        print(f"Converting {md_file} to PDF...")
        pdf.output(str(output_path))
        print(f"✓ PDF saved: {output_path}")
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description='Convert markdown to PDF')
    parser.add_argument('input', help='Input markdown file')
    parser.add_argument('-o', '--output', help='Output PDF file', default=None)
    
    args = parser.parse_args()
    parse_markdown_to_pdf(args.input, args.output)


if __name__ == "__main__":
    main()
