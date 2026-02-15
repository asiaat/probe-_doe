#!/usr/bin/env python3
"""
Markdown to PDF Converter
Converts markdown files to PDF using pandoc (preferred) or basic HTML conversion
"""

import os
import sys
import subprocess
import argparse
from pathlib import Path

def check_pandoc():
    """Check if pandoc is installed"""
    try:
        subprocess.run(['pandoc', '--version'], capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False

def convert_with_pandoc(md_file, output_dir=None):
    """Convert markdown to PDF using pandoc"""
    md_path = Path(md_file)
    
    if not md_path.exists():
        print(f"Error: File {md_file} not found")
        return False
    
    if output_dir:
        output_path = Path(output_dir) / f"{md_path.stem}.pdf"
    else:
        output_path = md_path.parent / f"{md_path.stem}.pdf"
    
    cmd = [
        'pandoc',
        str(md_path),
        '-o', str(output_path),
        '--pdf-engine=xelatex',
        '-V', 'mainfont=DejaVu Sans',
        '-V', 'geometry:margin=1in',
        '--toc',
        '--toc-depth=3'
    ]
    
    try:
        print(f"Converting {md_file} to PDF...")
        subprocess.run(cmd, check=True)
        print(f"✓ PDF saved: {output_path}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Pandoc error: {e}")
        return False

def convert_to_html(md_file, output_dir=None):
    """Convert markdown to HTML as fallback"""
    md_path = Path(md_file)
    
    if not md_path.exists():
        print(f"Error: File {md_file} not found")
        return False
    
    if output_dir:
        output_path = Path(output_dir) / f"{md_path.stem}.html"
    else:
        output_path = md_path.parent / f"{md_path.stem}.html"
    
    cmd = [
        'pandoc',
        str(md_path),
        '-o', str(output_path),
        '--standalone',
        '--self-contained'
    ]
    
    try:
        print(f"Converting {md_file} to HTML...")
        subprocess.run(cmd, check=True)
        print(f"✓ HTML saved: {output_path}")
        print("Note: Open HTML and use browser 'Save as PDF' for PDF conversion")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Pandoc error: {e}")
        return False

def convert_directory(dir_path, output_dir=None, file_pattern="*.md"):
    """Convert all markdown files in a directory"""
    dir_path = Path(dir_path)
    
    if not dir_path.exists():
        print(f"Error: Directory {dir_path} not found")
        return
    
    if output_dir:
        Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    md_files = list(dir_path.glob(file_pattern))
    
    if not md_files:
        print(f"No markdown files found in {dir_path}")
        return
    
    print(f"Found {len(md_files)} markdown files")
    
    success = 0
    for md_file in md_files:
        if check_pandoc():
            if convert_with_pandoc(md_file, output_dir):
                success += 1
        else:
            if convert_to_html(md_file, output_dir):
                success += 1
    
    print(f"\n✓ Converted {success}/{len(md_files)} files")

def main():
    parser = argparse.ArgumentParser(description='Convert markdown files to PDF')
    parser.add_argument('input', help='Markdown file or directory')
    parser.add_argument('-o', '--output', help='Output directory', default=None)
    parser.add_argument('-d', '--directory', help='Process entire directory', action='store_true')
    
    args = parser.parse_args()
    
    if args.directory or os.path.isdir(args.input):
        convert_directory(args.input, args.output)
    else:
        if check_pandoc():
            convert_with_pandoc(args.input, args.output)
        else:
            print("Pandoc not found. Converting to HTML instead.")
            convert_to_html(args.input, args.output)

if __name__ == "__main__":
    main()
