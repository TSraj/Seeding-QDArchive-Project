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
    Classify input text into ISIC Rev. 5 divisions (top 2 matches).
    
    Args:
        text: Input text (e.g., project title + description)
        
    Returns:
        tuple: (primary_section, primary_division, primary_score,
                secondary_section, secondary_division, secondary_score)
    """
    if not model or not text or not text.strip():
        return (None, None, 0.0, None, None, 0.0)
        
    # Get embedding for the input text
    text_embedding = model.encode(text, convert_to_tensor=True)
    
    # Compute cosine similarities
    cosine_scores = util.cos_sim(text_embedding, division_embeddings)[0]
    
    # Find the top 2 matches
    k = min(2, len(division_ids))
    top_scores, top_indices = torch.topk(cosine_scores, k)
    
    # Primary (best) match
    best_idx = top_indices[0].item()
    best_score = top_scores[0].item()
    primary_division = division_ids[best_idx]
    primary_section = DIVISIONS[primary_division]['section']
    
    # Secondary (2nd best) match
    if k >= 2:
        second_idx = top_indices[1].item()
        second_score = top_scores[1].item()
        secondary_division = division_ids[second_idx]
        secondary_section = DIVISIONS[secondary_division]['section']
    else:
        secondary_section, secondary_division, second_score = None, None, 0.0
    
    return (primary_section, primary_division, best_score,
            secondary_section, secondary_division, second_score)

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
        p_sec, p_div, p_score, s_sec, s_div, s_score = classify(text)
        print(f"\nText: {text}")
        print(f"Primary:   Section {p_sec}, Division {p_div} – {DIVISIONS[p_div]['title']} (Confidence: {p_score:.3f})")
        if s_div:
            print(f"Secondary: Section {s_sec}, Division {s_div} – {DIVISIONS[s_div]['title']} (Confidence: {s_score:.3f})")
