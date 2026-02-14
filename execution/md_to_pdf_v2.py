#!/usr/bin/env python3
"""
Markdown to PDF Converter using WeasyPrint
"""

import sys
import argparse
from pathlib import Path
from markdown2 import markdown
from weasyprint import HTML, CSS

def convert_md_to_pdf(md_file, output_file=None, css_file=None):
    """Convert markdown file to PDF"""
    
    md_path = Path(md_file)
    
    if not md_path.exists():
        print(f"Error: File {md_file} not found")
        return False
    
    # Read markdown content
    with open(md_path, 'r', encoding='utf-8') as f:
        md_content = f.read()
    
    # Convert markdown to HTML
    html_content = markdown(md_content)
    
    # Create full HTML document
    full_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>{md_path.stem}</title>
        <style>
            @page {{
                size: A4;
                margin: 2cm;
            }}
            body {{
                font-family: DejaVu Sans, Arial, sans-serif;
                font-size: 11pt;
                line-height: 1.5;
                color: #333;
            }}
            h1 {{
                font-size: 24pt;
                color: #1a1a1a;
                border-bottom: 2px solid #333;
                padding-bottom: 10px;
            }}
            h2 {{
                font-size: 18pt;
                color: #2a2a2a;
                margin-top: 30px;
            }}
            h3 {{
                font-size: 14pt;
                color: #3a3a3a;
                margin-top: 20px;
            }}
            table {{
                border-collapse: collapse;
                width: 100%;
                margin: 15px 0;
            }}
            th, td {{
                border: 1px solid #ddd;
                padding: 8px;
                text-align: left;
            }}
            th {{
                background-color: #f5f5f5;
                font-weight: bold;
            }}
            tr:nth-child(even) {{
                background-color: #fafafa;
            }}
            code {{
                background-color: #f4f4f4;
                padding: 2px 5px;
                border-radius: 3px;
                font-family: monospace;
            }}
            pre {{
                background-color: #f4f4f4;
                padding: 15px;
                border-radius: 5px;
                overflow-x: auto;
            }}
            blockquote {{
                border-left: 4px solid #ddd;
                margin: 15px 0;
                padding-left: 15px;
                color: #666;
            }}
            .classification {{
                background-color: #ffcccc;
                padding: 10px;
                border: 2px solid #cc0000;
                font-weight: bold;
                text-align: center;
                margin-bottom: 20px;
            }}
        </style>
    </head>
    <body>
        {html_content}
    </body>
    </html>
    """
    
    # Determine output path
    if output_file:
        output_path = Path(output_file)
    else:
        output_path = md_path.parent / f"{md_path.stem}.pdf"
    
    # Generate PDF
    try:
        print(f"Converting {md_file} to PDF...")
        html = HTML(string=full_html)
        html.write_pdf(output_path, stylesheets=[CSS(string='''
            @page { size: A4; margin: 2cm }
        ''')])
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
    convert_md_to_pdf(args.input, args.output)

if __name__ == "__main__":
    main()
