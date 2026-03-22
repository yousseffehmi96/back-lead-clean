import unicodedata
import re
import pandas as pd

def NetoyerUneChaine(text) -> str:
    # Gérer None et NaN
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return None
    
    # Convertir en string
    text = str(text).strip()
    if not text:
        return None
    
    # Enlever les accents
    nfd = unicodedata.normalize('NFD', text)
    ch = ""
    for i in nfd:
        if unicodedata.category(i) != "Mn":
            ch = ch + i
    
    ch = re.sub(r'[^a-zA-Z0-9\s]', '', ch).strip()
    
    if not ch:
        return None
    
    return ch[0].upper() + ch[1:]

def NetoyerUnNumero(text) -> str:
    # Gérer None et NaN
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return None
    
    # Convertir en string
    text = str(text).strip()
    if not text:
        return None
    
    ch = ""
    nfd = unicodedata.normalize('NFD', text)
    for i in nfd:
        if unicodedata.category(i) == "Nd":
            ch = ch + i
    
    return ch if ch else None

def NettoyerUnEmail(text) -> str:
    # Gérer None et NaN
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return None
    
    # Convertir en string
    text = str(text).strip()
    if not text:
        return None
    
    nfd = unicodedata.normalize('NFD', text)
    ch = ""
    for i in nfd:
        if unicodedata.category(i) != "Mn":
            ch = ch + i
    
    ch = re.sub(r'[^a-zA-Z0-9@._-]', '', ch).strip()
    
    if not ch:  
        return None
    
    if ch.startswith("@"):
        ch = ch[1:]
    if ch.endswith("@"):
        ch = ch[:-1]
    
    return ch.lower() if ch else None