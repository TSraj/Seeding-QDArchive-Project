"""
Step 3: Text Extractor

Handles extracting text from various document formats (Tier 2 extraction).
Supports .txt, .pdf, .docx, .rtf, and can extract text files hidden inside QDA archives (.qdpx, .nvp).
"""

import os
import zipfile
import docx
import pdfplumber
from striprtf.striprtf import rtf_to_text

# Maximum text length to extract per file (to prevent memory issues and keep embeddings focused)
MAX_TEXT_LENGTH = 10000

def extract_from_txt(filepath: str) -> str:
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            return f.read(MAX_TEXT_LENGTH)
    except Exception:
        return ""

def extract_from_docx(filepath: str) -> str:
    try:
        doc = docx.Document(filepath)
        fullText = []
        length = 0
        for para in doc.paragraphs:
            text = para.text.strip()
            if text:
                fullText.append(text)
                length += len(text)
                if length > MAX_TEXT_LENGTH:
                    break
        return '\\n'.join(fullText)[:MAX_TEXT_LENGTH]
    except Exception:
        return ""

def extract_from_pdf(filepath: str) -> str:
    try:
        text_parts = []
        length = 0
        with pdfplumber.open(filepath) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    text_parts.append(text)
                    length += len(text)
                    if length > MAX_TEXT_LENGTH:
                        break
        return '\\n'.join(text_parts)[:MAX_TEXT_LENGTH]
    except Exception:
        return ""

def extract_from_rtf(filepath: str) -> str:
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            rtf_content = f.read()
        text = rtf_to_text(rtf_content)
        return text[:MAX_TEXT_LENGTH]
    except Exception:
        return ""

def extract_from_zip(filepath: str) -> str:
    """
    Extracts text from the first few text-based files found inside a ZIP archive 
    (which QDA projects often are).
    """
    extracted_text = []
    length = 0
    try:
        with zipfile.ZipFile(filepath, 'r') as z:
            for info in z.infolist():
                # We only care about potential text files inside the archive
                ext = info.filename.lower().split('.')[-1]
                if ext in ['txt', 'rtf', 'csv', 'xml', 'json'] and info.file_size < 5000000:
                    try:
                        with z.open(info) as f:
                            content = f.read(4000).decode('utf-8', errors='ignore')
                            extracted_text.append(content)
                            length += len(content)
                            if length > MAX_TEXT_LENGTH:
                                break
                    except Exception:
                        continue
        return '\\n'.join(extracted_text)[:MAX_TEXT_LENGTH]
    except Exception:
        return ""

def extract_text(filepath: str) -> str:
    """
    Main extraction routing function. 
    Determines file type and calls appropriate extractor.
    """
    if not os.path.exists(filepath):
        return ""
        
    ext = filepath.lower().split('.')[-1]
    
    if ext == 'txt':
        return extract_from_txt(filepath)
    elif ext == 'docx':
        return extract_from_docx(filepath)
    elif ext == 'pdf':
        return extract_from_pdf(filepath)
    elif ext == 'rtf':
        return extract_from_rtf(filepath)
    elif ext in ['qdpx', 'nvp', 'nvpx', 'zip', 'mx22', 'mx24']:
        # QDA projects are often just zip files under the hood
        return extract_from_zip(filepath)
    else:
        return ""
