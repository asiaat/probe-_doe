#!/usr/bin/env python3
"""
Simple Markdown to PDF Converter
Uses basic text extraction - minimal dependencies
"""

import sys
import argparse
import re
from pathlib import Path

def simple_markdown_to_text(md_file):
    """Simple markdown to plain text converter"""
    
    md_path = Path(md_file)
    
    if not md_path.exists():
        return None
    
    with open(md_path, 'r', encoding='utf-8') as f:
        md_content = f.read()
    
    lines = md_content.split('\n')
    text_output = []
    
    for line in lines:
        line = line.strip()
        if not line:
            text_output.append('')
            continue
            
        # Headings
        if line.startswith('# '):
            text_output.append(f"\n{'='*60}\n{line[2:]}\n{'='*60}\n")
        elif line.startswith('## '):
            text_output.append(f"\n{'-'*60}\n{line[3:]}\n{'-'*60}\n")
        elif line.startswith('### '):
            text_output.append(f"\n**{line[4:]}**\n")
        elif line.startswith('#### '):
            text_output.append(f"\n*{line[5:]}*\n")
        # Tables - simplified
        elif line.startswith('|') and '---' not in line:
            cells = [c.strip() for c in line.split('|') if c.strip()]
            if cells:
                text_output.append(' | '.join(cells))
        # Lists
        elif line.startswith('- ') or line.startswith('* '):
            text_output.append(f"  • {line[2:]}")
        # Clean and add
        else:
            text = re.sub(r'\*\*([^*]+)\*\*', r'\1', line)
            text = re.sub(r'\*([^*]+)\*', r'\1', text)
            text = re.sub(r'`([^`]+)`', r'\1', text)
            text_output.append(text)
    
    return '\n'.join(text_output)


def main():
    parser = argparse.ArgumentParser(description='Simple markdown to text converter')
    parser.add_argument('input', help='Input markdown file')
    parser.add_argument('-o', '--output', help='Output text file', default=None)
    
    args = parser.parse_args()
    
    md_path = Path(args.input)
    if not md_path.exists():
        print(f"Error: File {args.input} not found")
        return
    
    text = simple_markdown_to_text(args.input)
    if text:
        output_path = args.output or str(md_path.parent / f"{md_path.stem}.txt")
        
        # Try to create PDF with reportlab if available
        try:
            from reportlab.lib.pagesizes import A4
            from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib.units import inch
            
            pdf_path = output_path.replace('.txt', '.pdf')
            
            doc = SimpleDocTemplate(pdf_path, pagesize=A4,
                                    rightMargin=72, leftMargin=72,
                                    topMargin=72, bottomMargin=18)
            
            styles = getSampleStyleSheet()
            story = []
            
            for line in text.split('\n'):
                if not line.strip():
                    story.append(Spacer(1, 0.2*inch))
                elif '=' in line and len(line) < 70:
                    # Title
                    p = ParagraphStyle('Title', parent=styles['Title'], fontSize=16)
                    story.append(Paragraph(line.replace('=', '').strip(), p))
                    story.append(Spacer(1, 0.3*inch))
                elif '-' in line and len(line) < 70:
                    # Section header
                    p = ParagraphStyle('Section', parent=styles['Heading2'], fontSize=12)
                    story.append(Paragraph(line.replace('-', '').strip(), p))
                else:
                    story.append(Paragraph(line, styles['BodyText']))
            
            doc.build(story)
            print(f"✓ PDF saved: {pdf_path}")
            return
        except ImportError:
            pass
        
        # Fallback to text file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(text)
        
        print(f"✓ Text file saved: {output_path}")
        print("Note: Install reportlab for PDF output: pip install reportlab")


if __name__ == "__main__":
    main()
