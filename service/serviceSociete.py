from sqlalchemy.orm import Session
from model.societe_leads import societeleads
from fastapi import HTTPException
from schema.schemaSociete import Societe
from model.staging_leads import StagingLeads
from sqlalchemy import func,text
from sqlalchemy.exc import SQLAlchemyError
import re
import unicodedata


def _norm_name(v) -> str:
    """minuscule, sans accents, sans espaces (pour comparer prénom/nom)."""
    if not v:
        return ""
    s = str(v).strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"\s+", "", s)
    return s


def derive_patterne(email, prenom, nom) -> str:
    """
    Déduit un template d'email (patterne) à partir d'un email réel + prénom/nom.
    Reconnaît prénom/nom complets, initiales, ordre et séparateur.
    Ex: 'j.doe@soprat.fr' (John Doe) -> '{p}.{nom}@soprat.fr'
        'djohn@soprat.fr'            -> '{n}{prenom}@soprat.fr'
    """
    e = str(email or "").strip().lower()
    if "@" not in e:
        return e
    local, domain = e.split("@", 1)
    local = re.sub(r"\s+", "", local)

    p_full = _norm_name(prenom)
    n_full = _norm_name(nom)
    pi, ni = p_full[:1], n_full[:1]

    # séparateur présent dans la partie locale
    sep = ""
    for s in (".", "_", "-"):
        if s in local:
            sep = s
            break

    # candidats (template, valeur) du plus spécifique au moins spécifique
    candidates = [
        ("{prenom}" + sep + "{nom}", p_full + sep + n_full),
        ("{nom}" + sep + "{prenom}", n_full + sep + p_full),
        ("{p}" + sep + "{nom}", pi + sep + n_full),
        ("{nom}" + sep + "{p}", n_full + sep + pi),
        ("{n}" + sep + "{prenom}", ni + sep + p_full),
        ("{prenom}" + sep + "{n}", p_full + sep + ni),
        ("{p}" + sep + "{n}", pi + sep + ni),
        ("{n}" + sep + "{p}", ni + sep + pi),
        ("{prenom}", p_full),
        ("{nom}", n_full),
    ]
    for template, value in candidates:
        if value and value == local:
            return template + "@" + domain

    # Inférence par position : local-part "A<sep>B" (2 segments alphabétiques).
    # Si un seul côté matche un nom, on déduit l'autre comme le nom complémentaire.
    if sep and local.count(sep) == 1:
        a, b = local.split(sep, 1)
        if re.fullmatch(r"[a-z]+", a) and re.fullmatch(r"[a-z]+", b):
            def classify(seg):
                if p_full and seg == p_full: return "{prenom}", "prenom"
                if n_full and seg == n_full: return "{nom}", "nom"
                if pi and seg == pi:         return "{p}", "prenom"
                if ni and seg == ni:         return "{n}", "nom"
                return None, None
            ta, side_a = classify(a)
            tb, side_b = classify(b)
            if ta and tb:
                return ta + sep + tb + "@" + domain
            if ta and not tb:                                   # A reconnu -> B = complémentaire
                tb = "{nom}" if side_a == "prenom" else "{prenom}"
                return ta + sep + tb + "@" + domain
            if tb and not ta:                                   # B reconnu -> A = complémentaire
                ta = "{nom}" if side_b == "prenom" else "{prenom}"
                return ta + sep + tb + "@" + domain

    # fallback: remplacer les noms complets si présents, sinon garder tel quel
    fallback = local
    if p_full:
        fallback = fallback.replace(p_full, "{prenom}")
    if n_full:
        fallback = fallback.replace(n_full, "{nom}")
    return fallback + "@" + domain

def AddSoc(societe: Societe, db: Session):
    """
    Crée une société manuellement via POST /societe.
    """
    try:
        data = societe.dict()
        nom = (data.get("nom") or "").strip()
        patterne = (data.get("patterne") or "").strip()
        regex = (data.get("regex") or "").strip()

        if not nom:
            raise HTTPException(status_code=400, detail="Le nom de la société est obligatoire")

        exists = db.query(societeleads).filter(func.lower(societeleads.nom) == nom.lower()).first()
        if exists:
            raise HTTPException(status_code=409, detail="Société déjà existante")

        obj = societeleads(nom=nom, patterne=patterne, regex=regex)
        db.add(obj)
        db.commit()
        db.refresh(obj)
        return {"message": "Société ajoutée", "id": obj.id}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur serveur: {str(e)}")
def AddAuto(db: Session, base: str):
    """
    Ajoute automatiquement les sociétés manquantes depuis une table de leads.
    (ex: staging_leads, silver_leads, etc.)
    """
    try:
        # Un représentant par société (priorité aux lignes avec prénom ET nom non vides).
        rows = db.execute(text(f"""
            SELECT DISTINCT ON (LOWER(TRIM(societe)))
                TRIM(societe) AS nom,
                email, prenom, nom AS nom_lead
            FROM {base}
            WHERE email IS NOT NULL AND email != '' AND LOWER(email) != 'nan'
              AND societe IS NOT NULL AND societe != '' AND LOWER(societe) != 'nan'
            ORDER BY
                LOWER(TRIM(societe)),
                (CASE WHEN COALESCE(prenom,'') <> '' AND COALESCE(nom,'') <> '' THEN 0 ELSE 1 END),
                id
        """)).mappings().all()

        # Sociétés déjà présentes (comparaison insensible à la casse)
        existing = {
            str(r[0]).strip().lower()
            for r in db.execute(text("SELECT nom FROM societe_leads")).all()
            if r[0]
        }

        added_count = 0
        seen = set()
        for r in rows:
            nom_soc = (r["nom"] or "").strip()
            key = nom_soc.lower()
            if not nom_soc or key in existing or key in seen:
                continue
            patterne = derive_patterne(r["email"], r["prenom"], r["nom_lead"])
            res = db.execute(
                text("INSERT INTO societe_leads (nom, patterne) VALUES (:nom, :patterne) ON CONFLICT (nom) DO NOTHING"),
                {"nom": nom_soc, "patterne": patterne},
            )
            seen.add(key)
            added_count += int(res.rowcount or 0)

        db.commit()
        return {"added_societes": int(added_count or 0)}
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")


def DeleteSociete(id: int, db: Session):
    try:
        societe = db.query(societeleads).filter(societeleads.id == id).first()

        if not societe:
            raise HTTPException(status_code=404, detail="Non trouvé")

        db.delete(societe)
        db.commit()
        return {"message": "Suppression réussie"}

    except Exception:
        db.rollback()
        raise HTTPException(status_code=500, detail="Erreur serveur")

def UpdateSociete(id: int, societe_data:Societe, db: Session):
    try:
        data = societe_data.dict(exclude_unset=True)

        result = db.query(societeleads).filter(
            societeleads.id == id
        )

        if not result.first():
            raise HTTPException(status_code=404, detail="Non trouvé")

        result.update(data)
        db.commit()

        return {"message": "Modification réussie"}

    except Exception:
        db.rollback()
        raise HTTPException(status_code=500, detail="Erreur serveur")

def GetAll(db:Session):
    return db.query(societeleads).all()

## NOTE: l'ancienne implémentation AddAuto dupliquée a été supprimée.

def get_domain(email: str):
    if not email or email.lower() in ("nan", "none", "null", ""):
        return None, None
    try:
        domain = email.split("@")[1]     
        parts = domain.split(".")
        name = parts[0]                   
        extension = parts[-1]            
        return name, extension
    except IndexError:
        return None, None