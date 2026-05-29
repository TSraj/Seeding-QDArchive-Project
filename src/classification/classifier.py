"""
Step 2: ISIC Rev. 5 Classifier

Uses sentence-transformers to embed text and classify it into one of the 87
ISIC Rev. 5 divisions based on cosine similarity.

Usage:
    from src.classification.classifier import classify, generate_tags
"""

from sentence_transformers import SentenceTransformer, util
import torch
import json
import os
try:
    from .isic_taxonomy import DIVISIONS
except ImportError:
    from src.classification.isic_taxonomy import DIVISIONS

# Initialize model
MODEL_NAME = 'all-MiniLM-L6-v2'
try:
    print(f"Loading {MODEL_NAME} model...")
    model = SentenceTransformer(MODEL_NAME)
except Exception as e:
    print(f"Failed to load model {MODEL_NAME}: {e}")
    # Will fail if dependencies are missing, but we handle it gracefully for now.
    model = None

# Pre-compute division embeddings
division_ids = list(DIVISIONS.keys())
division_descriptions = [DIVISIONS[did]['full_description'] for did in division_ids]

if model:
    print("Pre-computing division embeddings...")
    division_embeddings = model.encode(division_descriptions, convert_to_tensor=True)
else:
    division_embeddings = None

def classify(text: str) -> tuple:
    """
    Classify input text into an ISIC Rev. 5 division.
    
    Args:
        text: Input text (e.g., project title + description)
        
    Returns:
        tuple: (section_code, division_code, confidence_score)
    """
    if not model or not text or not text.strip():
        return (None, None, 0.0)
        
    # Get embedding for the input text
    text_embedding = model.encode(text, convert_to_tensor=True)
    
    # Compute cosine similarities
    cosine_scores = util.cos_sim(text_embedding, division_embeddings)[0]
    
    # Find the best match
    best_idx = torch.argmax(cosine_scores).item()
    best_score = cosine_scores[best_idx].item()
    
    division_code = division_ids[best_idx]
    section_code = DIVISIONS[division_code]['section']
    
    return (section_code, division_code, best_score)

def generate_tags(text: str, existing_keywords: str) -> list:
    """
    Generate tags based on text and existing keywords.
    Since we don't have an LLM, we will just parse and normalize existing keywords
    and maybe extract some basic noun phrases or frequent terms if needed.
    For this purely descriptive pipeline, returning the existing DB keywords 
    as a clean list is sufficient.
    
    Args:
        text: Input text
        existing_keywords: Keywords from the database
        
    Returns:
        list of tags/keywords
    """
    if not existing_keywords:
        return []
        
    # Simple split by comma or semicolon
    raw_tags = existing_keywords.replace(';', ',').split(',')
    tags = [tag.strip().lower() for tag in raw_tags if tag.strip()]
    return list(set(tags))

if __name__ == '__main__':
    # Test the classifier
    sample_texts = [
        "Through doctors' eyes: A qualitative study of hospital doctor perspectives on their working conditions",
        "Agricultural crop yield data and farming methods in rural communities",
        "Software development practices in open source communities"
    ]
    
    for text in sample_texts:
        section, division, score = classify(text)
        print(f"\\nText: {text}")
        print(f"Classification: Section {section}, Division {division} (Confidence: {score:.3f})")
        print(f"Division Name: {DIVISIONS[division]['title']}")
