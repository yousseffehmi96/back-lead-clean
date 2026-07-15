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
    # Nettoyage des séparateurs parasites : doublons ("a..b"->"a.b") + tête/queue (".a."->"a")
    local = re.sub(r"([._-])[._-]+", r"\1", local)
    local = local.strip("._-")
    domain = domain.strip(".")

    def _norm(v):
        s = unicodedata.normalize("NFD", str(v or "").lower())
        return "".join(c for c in s if unicodedata.category(c) != "Mn")

    def _words(v):
        # mots alphanumériques (gère prénoms/noms composés : "Ait Hmid", "Ait-Hmid" -> ["ait","hmid"])
        return [w for w in re.split(r"[^a-z0-9]+", _norm(v)) if w]

    p_words = _words(prenom)
    n_words = _words(nom)
    p_full = "".join(p_words)
    n_full = "".join(n_words)
    pi, ni = p_full[:1], n_full[:1]

    def _name_variants(ws):
        """Regex candidates pour un nom, du plus strict au plus tolérant."""
        if not ws:
            return []
        # les mots peuvent être collés OU séparés par . _ - dans l'email (ait-hmid, ait.hmid, aithmid)
        variants = [r"[._-]?".join(re.escape(w) for w in ws)]
        # Repli : le nom est stocké en un seul mot alors que l'email le sépare.
        # Cas réel : "Ait-Hmid" nettoyé en "AitHmid" à l'import, mais l'email
        # reste "ait-hmid" -> sans ce repli, {nom} n'est pas placé et le nom de
        # la personne se retrouve figé en dur dans le patterne de la société.
        if len(ws) == 1 and len(ws[0]) >= 4:
            variants.append(r"[._-]?".join(re.escape(c) for c in ws[0]))
        return variants

    result = local
    placed = {"p": False, "n": False}

    # 1) Remplacer les noms COMPLETS (le plus long d'abord pour éviter les chevauchements)
    for k, ws, length in sorted(
        [("n", n_words, len(n_full)), ("p", p_words, len(p_full))], key=lambda x: -x[2]
    ):
        token = "{nom}" if k == "n" else "{prenom}"
        for rx in _name_variants(ws):
            new = re.sub(rx, token, result, count=1)
            if new != result:
                result = new
                placed[k] = True
                break

    # 2) Initiales : un segment d'UNE seule lettre égal à l'initiale (si le nom complet n'a pas été placé)
    def _repl_initial(s, letter, token):
        return re.sub(r"(?<![a-z0-9])" + re.escape(letter) + r"(?![a-z0-9])", token, s, count=1)

    if not placed["p"] and pi:
        new = _repl_initial(result, pi, "{p}")
        if new != result:
            result, placed["p"] = new, True
    if not placed["n"] and ni:
        new = _repl_initial(result, ni, "{n}")
        if new != result:
            result, placed["n"] = new, True

    # 3) Inférence par position : "A<sep>B", un seul côté reconnu -> l'autre = complémentaire
    #    (utile quand un prénom/nom est vide ou incohérent en base)
    TOK = r"\{prenom\}|\{nom\}|\{p\}|\{n\}"
    m = re.fullmatch(rf"({TOK}|[a-z0-9]+)([._-])({TOK}|[a-z0-9]+)", result)
    if m:
        a, sep, b = m.group(1), m.group(2), m.group(3)
        a_t, b_t = a.startswith("{"), b.startswith("{")
        if a_t ^ b_t:  # exactement un côté est un token
            if a_t:
                result = a + sep + ("{nom}" if a in ("{prenom}", "{p}") else "{prenom}")
            else:
                result = ("{nom}" if b in ("{prenom}", "{p}") else "{prenom}") + sep + b

    return result + "@" + domain

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
    (ex: import_leads, leads, etc.)
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