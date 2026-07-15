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
    
    # On conserve le trait d'union et l'apostrophe : ils font partie des noms
    # ("Ait-Hmid", "Jean-Luc", "D'Angelo"). Les supprimer cassait la déduction
    # du patterne société (aithmid ne matche pas ait-hmid dans l'email).
    ch = re.sub(r"[^a-zA-Z0-9\s'-]", '', ch).strip()
    # Nettoie les séparateurs isolés ou en trop (" - ", "--", tête/queue)
    ch = re.sub(r"\s*-\s*", "-", ch)
    ch = re.sub(r"([-'])[-']+", r"\1", ch).strip(" -'")
    
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