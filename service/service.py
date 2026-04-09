from fastapi import UploadFile, HTTPException
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text, bindparam
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy import Text
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional
from model.staging_leads import StagingLeads
from model.blacklistLeads import blacklistLeads
from model.cleaning_leads import cleaningleads
from model.societe_leads import societeleads
from model.silver_leads import Silver_leads
from util.util import NetoyerUneChaine, NetoyerUnNumero, NettoyerUnEmail
from model.statistiqueLeads import StatisticLeads
from schema.schemaStatic import Static 
import unicodedata
import openpyxl
import io

def ensure_app_settings_table(db: Session):
    db.execute(text("""
        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """))
    db.commit()

def GetEmailPattern(db: Session) -> str:
    ensure_app_settings_table(db)
    row = db.execute(text("SELECT value FROM app_settings WHERE key = 'email_pattern'")).fetchone()
    if row and row[0]:
        return str(row[0])
    return "{prenom}.{nom}@{domaine}.{extension}"

def SaveEmailPattern(db: Session, pattern: str, is_manager: bool):
    if not is_manager:
        raise HTTPException(status_code=403, detail="Accès refusé: manager seulement")
    ensure_app_settings_table(db)
    normalized = _normalize_email_pattern(pattern)
    db.execute(text("""
        INSERT INTO app_settings(key, value, updated_at)
        VALUES ('email_pattern', :val, CURRENT_TIMESTAMP)
        ON CONFLICT (key)
        DO UPDATE SET value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP
    """), {"val": normalized})
    db.commit()
    return {"message": "Pattern enregistré", "pattern": normalized}


def normalize_col(col: str) -> str:
    result = unicodedata.normalize('NFKD', str(col)).encode('ascii', 'ignore').decode('ascii').strip().lower()
    result = result.replace("'", " ").replace("'", " ").replace("`", " ")
    return result
def LoadFileToBd(file: UploadFile, db: Session, userid: Optional[str] = None, username: Optional[str] = None):
    COLUMN_ALIASES = {
        "Nom":       ["nom", "last name", "lastname", "last_name", "surname", "nom du contact"],
        "Prenom":    ["prenom", "first name", "firstname", "first_name", "name", "prenom du contact"],
        "Email":     ["email", "mail", "e-mail", "courriel", "adresse mail"],
        "Fonction":  ["fonction", "title", "titre", "poste", "job title"],
        "Societe":   ["societe", "company", "company name", "entreprise", "organization", "nom de l entreprise"],
        "Telephone": ["telephone", "tel", "phone", "mobile", "gsm"],
        "Linkedin":  ["linkedin", "cnx et msg linkedin"],
        "Location":  ["location", "lieu", "ville", "pays"],
    }
 
    try:
        
        file.file.seek(0)
 
        # 📄 CSV
        if file.filename.endswith(".csv"):
            raw = file.file.read()
            try:
                decoded_text = raw.decode("utf-8")
            except UnicodeDecodeError:
                decoded_text = raw.decode("latin1")
 
            sep = ","
            for delimiter in [",", ";", "\t", "|"]:
                try:
                    test_df = pd.read_csv(io.StringIO(decoded_text), sep=delimiter, nrows=5, dtype=str)
                    if len(test_df.columns) > 2:
                        sep = delimiter
                        break
                except:
                    continue
 
            df = pd.read_csv(io.StringIO(decoded_text), sep=sep, engine="python", dtype=str)
 
        # 📊 Excel
        elif file.filename.endswith((".xlsx", ".xls")):
            df = pd.read_excel(file.file, dtype=str, sheet_name=0) 
            # Supprimer les colonnes Unnamed
            df = df.loc[:, ~df.columns.str.startswith('Unnamed')]
            print(f"📊 Lignes: {len(df)}, Colonnes: {df.columns.tolist()}")

        else:
            raise HTTPException(status_code=400, detail="Format non supporté. Utilisez CSV, XLSX ou XLS.")
 
        # 🔄 Normaliser les noms de colonnes (enlever accents + minuscules + strip espaces)
        df.columns = [normalize_col(col).strip() for col in df.columns]
        print("Colonnes normalisées:", df.columns.tolist())
 
        # 🔄 Mapping colonnes
        # Priorité: on prend le 1er alias trouvé selon l'ordre défini dans COLUMN_ALIASES.
        # Ex: si "prenom" et "first name" existent ensemble, "prenom" est gardé.
        rename_map = {}
        normalized_aliases = {
            standard_name: [normalize_col(alias).strip() for alias in aliases]
            for standard_name, aliases in COLUMN_ALIASES.items()
        }

        for standard_name, aliases in normalized_aliases.items():
            matched_col = None
            for alias in aliases:
                if alias in df.columns:
                    matched_col = alias
                    break
            if matched_col:
                rename_map[matched_col] = standard_name
 
        df.rename(columns=rename_map, inplace=True)
        print(f"📋 Colonnes après rename: {df.columns.tolist()}")
        
        # ✅ SUPPRIMER LES COLONNES DUPLIQUÉES (garder la première occurrence)
        df = df.loc[:, ~df.columns.duplicated(keep='first')]
        print(f"🔧 Colonnes après suppression des doublons: {df.columns.tolist()}")
 
        # ➕ Ajouter colonnes manquantes
        for col in ["Nom", "Prenom", "Email", "Fonction", "Societe", "Telephone", "Linkedin", "Location"]:
            if col not in df.columns:
                df[col] = None
 
        # ✅ SÉLECTIONNER UNIQUEMENT LES 8 COLONNES NÉCESSAIRES
        df_clean = df[["Nom", "Prenom", "Email", "Fonction", "Societe", "Telephone", "Linkedin", "Location"]].copy()
        
        print(f"✅ Colonnes sélectionnées: {df_clean.columns.tolist()} (Total: {len(df_clean.columns)})")

        # Appliquer get_val
        def get_val(val):
            if val is None:
                return None
            if isinstance(val, float) and pd.isna(val):
                return None
            s = str(val).strip()
            if s.lower() in ("nan", "none", "null", "", "n/a"):
                return None
            return s[:255]

        df_clean = df_clean.map(get_val)
        
        # Renommer les colonnes en minuscules pour le mapping
        df_clean.columns = ["nom", "prenom", "email", "fonction", "societe", "telephone", "linkedin", "location"]

        # 🧹 NETTOYAGE AVANT INSERTION
        print("🧹 Nettoyage des données...")
        
        df_clean['nom'] = df_clean['nom'].apply(NetoyerUneChaine)
        df_clean['prenom'] = df_clean['prenom'].apply(NetoyerUneChaine)
        df_clean['fonction'] = df_clean['fonction'].apply(NetoyerUneChaine)
        df_clean['societe'] = df_clean['societe'].apply(NetoyerUneChaine)
        df_clean['location'] = df_clean['location'].apply(NetoyerUneChaine)
        df_clean['telephone'] = df_clean['telephone'].apply(NetoyerUnNumero)
        df_clean['email'] = df_clean['email'].apply(NettoyerUnEmail)

        print("✅ Nettoyage terminé")

        # ✅ Check "déjà traité" basé sur Historique des imports (staging_import_history)
        # On compare le contenu (principalement emails) AVANT d'insérer en staging/history.
        if userid:
            unique_emails = sorted({(e or "").strip().lower() for e in df_clean["email"].tolist() if e and str(e).strip().lower() not in ("", "nan")})
            if unique_emails:
                q = text("""
                    SELECT COUNT(DISTINCT LOWER(TRIM(email)))
                    FROM staging_import_history
                    WHERE iduser = :userid
                      AND LOWER(TRIM(email)) = ANY(:emails)
                """).bindparams(bindparam("emails", type_=ARRAY(Text)))
                existing = db.execute(q, {"userid": str(userid), "emails": unique_emails}).scalar() or 0
                existing = int(existing)
                # si (quasi) tous les emails sont déjà dans l'historique, considérer le fichier déjà traité
                if existing >= max(1, int(0.95 * len(unique_emails))):
                    return {
                        "inserted_rows": 0,
                        "duplicate_file_processed": True,
                        "already_processed_in_history": existing,
                        "message": "Tu as deja traite ce fichier"
                    }

        # 🚀 Insertion ultra-rapide
        engine = db.get_bind()
        df_clean.to_sql(
            name='staging_leads',
            con=engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )

        # Historiser chaque import pour garder la trace des leads importes
        # Ajout optionnel des infos utilisateur (utile pour export manager)
        db.execute(text("ALTER TABLE staging_import_history ADD COLUMN IF NOT EXISTS username TEXT"))
        db.commit()
        history_df = df_clean.copy()
        history_df["filename"] = file.filename
        history_df["iduser"] = userid
        history_df["username"] = username
        history_df = history_df[
            ["filename", "iduser", "username", "nom", "prenom", "email", "fonction", "societe", "telephone", "linkedin", "location"]
        ]
        history_df.to_sql(
            name='staging_import_history',
            con=engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        print(f"✅ {len(df_clean)} lignes insérées et nettoyées avec succès")
        
        return {"inserted_rows": len(df_clean)}

 
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur DB : {str(e)}")
 
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur : {str(e)}")
def SupprimerDoublonsMemetABLE(db: Session, table: str):
    try:
        allowed_tables = {"staging_leads", "cleaning_leads", "silver_leads"}
        if table not in allowed_tables:
            raise HTTPException(status_code=400, detail=f"Table '{table}' non autorisée.")

        query = text(f"""
            DELETE FROM {table}
            WHERE id NOT IN (
                SELECT MIN(id)
                FROM {table}
                GROUP BY 
                    COALESCE(nom, ''),
                    COALESCE(prenom, ''),
                    COALESCE(email, ''),
                    COALESCE(fonction, ''),
                    COALESCE(societe, '')
            )
        """)
        res = db.execute(query)
        db.commit()
        return {"duplicates_deleted": res.rowcount if hasattr(res, "rowcount") else 0}

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur suppression doublons : {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")
def SupprimerDoublons(db: Session):
    try:
        results = {}
        total_deleted = 0

        query_staging_silver = text("""
            DELETE FROM staging_leads
            WHERE id IN (
                SELECT s.id
                FROM staging_leads s
                INNER JOIN silver_leads sl ON 
                    COALESCE(s.email, '') = COALESCE(sl.email, '') AND
                    COALESCE(s.nom, '') = COALESCE(sl.nom, '') AND
                    COALESCE(s.prenom, '') = COALESCE(sl.prenom, '')
                WHERE s.email IS NOT NULL AND s.email != ''
            )
        """)
        res1 = db.execute(query_staging_silver)
        staging_vs_silver = res1.rowcount if hasattr(res1, "rowcount") else 0
        results["staging_vs_silver"] = staging_vs_silver
        total_deleted += staging_vs_silver
        print(f"✅ STAGING vs SILVER: {staging_vs_silver} doublons supprimés")

        query_staging_gold = text("""
            DELETE FROM staging_leads
            WHERE id IN (
                SELECT s.id
                FROM staging_leads s
                INNER JOIN gold_leads g ON 
                    COALESCE(s.email, '') = COALESCE(g.email, '') AND
                    COALESCE(s.nom, '') = COALESCE(g.nom, '') AND
                    COALESCE(s.prenom, '') = COALESCE(g.prenom, '')
                WHERE s.email IS NOT NULL AND s.email != ''
            )
        """)
        res2 = db.execute(query_staging_gold)
        staging_vs_gold = res2.rowcount if hasattr(res2, "rowcount") else 0
        results["staging_vs_gold"] = staging_vs_gold
        total_deleted += staging_vs_gold
        print(f"✅ STAGING vs GOLD: {staging_vs_gold} doublons supprimés")

        query_staging_applique = text("""
            DELETE FROM staging_leads
            WHERE id IN (
                SELECT s.id
                FROM staging_leads s
                INNER JOIN steaging_applique sa ON
                    COALESCE(s.email, '') = COALESCE(sa.email, '') AND
                    COALESCE(s.nom, '') = COALESCE(sa.nom, '') AND
                    COALESCE(s.prenom, '') = COALESCE(sa.prenom, '')
                WHERE s.email IS NOT NULL AND s.email != ''
            )
        """)
        res3 = db.execute(query_staging_applique)
        staging_vs_applique = res3.rowcount if hasattr(res3, "rowcount") else 0
        results["staging_vs_applique"] = staging_vs_applique
        total_deleted += staging_vs_applique
        print(f"✅ STAGING vs STEAGING_APPLIQUE: {staging_vs_applique} doublons supprimés")


        # 4️⃣ Supprimer doublons dans STAGING lui-même
        query_staging_internal = text("""
            DELETE FROM staging_leads
            WHERE id NOT IN (
                SELECT MIN(id)
                FROM staging_leads
                GROUP BY 
                    COALESCE(nom, ''),
                    COALESCE(prenom, ''),
                    COALESCE(email, ''),
                    COALESCE(fonction, ''),
                    COALESCE(societe, '')
            )
        """)
        res4 = db.execute(query_staging_internal)
        staging_internal = res4.rowcount if hasattr(res4, "rowcount") else 0
        results["staging_internal"] = staging_internal
        total_deleted += staging_internal
        print(f"✅ STAGING interne: {staging_internal} doublons supprimés")

        db.commit()
        
        return {
            "message": "Suppression des doublons terminée",
            "total_deleted": total_deleted,
            "staging_vs_silver": staging_vs_silver,
            "staging_vs_gold": staging_vs_gold,
            "staging_vs_applique": staging_vs_applique,
            "staging_internal": staging_internal
        }

    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur suppression doublons : {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")


def _normalize_email_pattern(pattern: Optional[str]) -> str:
    # Tokens supportés: {prenom} {nom} {domaine} {extension}
    default_pattern = "{prenom}.{nom}@{domaine}.{extension}"
    if not pattern:
        return default_pattern
    p = str(pattern).strip()
    if not p:
        return default_pattern
    if len(p) > 200:
        raise HTTPException(status_code=400, detail="Pattern email trop long")
    required = ["{prenom}", "{nom}", "{domaine}", "{extension}"]
    if any(tok not in p for tok in required):
        raise HTTPException(status_code=400, detail="Pattern invalide. Tokens requis: {prenom} {nom} {domaine} {extension}")
    if "@" not in p:
        raise HTTPException(status_code=400, detail="Pattern invalide: '@' manquant")
    return p

def CompleteEmail(db: Session,base:str, pattern: Optional[str] = None, overwrite: bool = False):
    print("🔄 Début de complétion des emails...")
    try:
        from sqlalchemy import text
        pattern = _normalize_email_pattern(pattern)

        # Expression SQL pour l'email cible (utilisée aussi dans NOT EXISTS anti-doublon)
        target_email_from_societe = """
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(:pattern, '{prenom}', LOWER(REGEXP_REPLACE(sl.prenom, '\\s+', '', 'g'))),
                    '{nom}', LOWER(REGEXP_REPLACE(sl.nom, '\\s+', '', 'g'))),
                '{domaine}', s.domaine),
            '{extension}', s.extension)
        """
        target_email_from_existing = """
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(:pattern, '{prenom}', LOWER(REGEXP_REPLACE(sl.prenom, '\\s+', '', 'g'))),
                    '{nom}', LOWER(REGEXP_REPLACE(sl.nom, '\\s+', '', 'g'))),
                '{domaine}', SPLIT_PART(SPLIT_PART(sl.email, '@', 2), '.', 1)),
            '{extension}', SPLIT_PART(SPLIT_PART(sl.email, '@', 2), '.', -1))
        """

        # Exécuter en une seule requête (CTE) en résolvant les collisions (societe + fallback):
        # - On construit tous les candidats d'email (2 sources)
        # - On garde UNE seule ligne par email (score desc, source prioritaire, id asc)
        # - On supprime les autres lignes en collision
        # - On met à jour le gagnant si l'email n'existe pas déjà sur une autre ligne
        filled_score = """
            (
                (CASE WHEN sl.nom IS NOT NULL AND sl.nom != '' AND sl.nom != 'nan' THEN 1 ELSE 0 END) +
                (CASE WHEN sl.prenom IS NOT NULL AND sl.prenom != '' AND sl.prenom != 'nan' THEN 1 ELSE 0 END) +
                (CASE WHEN sl.fonction IS NOT NULL AND sl.fonction != '' AND sl.fonction != 'nan' THEN 1 ELSE 0 END) +
                (CASE WHEN sl.societe IS NOT NULL AND sl.societe != '' AND sl.societe != 'nan' THEN 1 ELSE 0 END) +
                (CASE WHEN sl.telephone IS NOT NULL AND sl.telephone != '' AND sl.telephone != 'nan' THEN 1 ELSE 0 END) +
                (CASE WHEN sl.linkedin IS NOT NULL AND sl.linkedin != '' AND sl.linkedin != 'nan' THEN 1 ELSE 0 END) +
                (CASE WHEN sl.location IS NOT NULL AND sl.location != '' AND sl.location != 'nan' THEN 1 ELSE 0 END)
            )
        """
        count_row = db.execute(text(f"""
            WITH candidates1 AS (
                SELECT
                    sl.id,
                    {target_email_from_societe} AS new_email,
                    {filled_score} AS score,
                    1 AS source
                FROM {base} sl
                JOIN societe_leads s ON UPPER(sl.societe) = UPPER(s.nom)
                WHERE (
                        :overwrite = TRUE
                        OR (sl.email IS NULL OR sl.email = '' OR sl.email = 'nan')
                  )
                  AND sl.nom IS NOT NULL AND sl.nom != '' AND sl.nom != 'nan'
                  AND sl.prenom IS NOT NULL AND sl.prenom != '' AND sl.prenom != 'nan'
                  AND s.domaine IS NOT NULL
                  AND s.extension IS NOT NULL
            ),
            candidates2 AS (
                SELECT
                    sl.id,
                    {target_email_from_existing} AS new_email,
                    {filled_score} AS score,
                    2 AS source
                FROM {base} sl
                WHERE :overwrite = TRUE
                  AND sl.email IS NOT NULL
                  AND sl.email != ''
                  AND LOWER(sl.email) != 'nan'
                  AND sl.email LIKE '%@%.%'
                  AND sl.nom IS NOT NULL AND sl.nom != '' AND sl.nom != 'nan'
                  AND sl.prenom IS NOT NULL AND sl.prenom != '' AND sl.prenom != 'nan'
            ),
            candidates AS (
                SELECT * FROM candidates1
                UNION ALL
                SELECT * FROM candidates2
            ),
            candidates_clean AS (
                SELECT *
                FROM candidates
                WHERE new_email IS NOT NULL AND new_email != '' AND LOWER(new_email) != 'nan'
            ),
            ranked AS (
                SELECT
                    id,
                    new_email,
                    score,
                    source,
                    ROW_NUMBER() OVER (
                        PARTITION BY new_email
                        ORDER BY score DESC, source ASC, id ASC
                    ) AS rn
                FROM candidates_clean
            ),
            dupe_delete AS (
                DELETE FROM {base} d
                USING ranked r
                WHERE d.id = r.id
                  AND r.rn > 1
                RETURNING d.id
            ),
            winners AS (
                SELECT id, new_email
                FROM ranked
                WHERE rn = 1
            ),
            u AS (
                UPDATE {base} sl
                SET email = w.new_email
                FROM winners w
                WHERE sl.id = w.id
                  AND NOT EXISTS (
                        SELECT 1 FROM {base} x
                        WHERE x.email = w.new_email
                          AND x.id <> sl.id
                  )
                RETURNING sl.id
            )
            SELECT
                (SELECT COUNT(*) FROM u) AS emails_completed,
                (SELECT COUNT(*) FROM dupe_delete) AS deleted_collisions
        """), {"pattern": pattern, "overwrite": overwrite}).mappings().first()
        
        db.commit()
        emails_completed = int((count_row or {}).get("emails_completed", 0) or 0)
        deleted_collisions = int((count_row or {}).get("deleted_collisions", 0) or 0)
        
        print(f"✅ {emails_completed} emails complétés")
        return {"emails_completed": emails_completed, "deleted_collisions": deleted_collisions}

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        db.rollback()
        msg = str(e)
        if "UniqueViolation" in msg or "duplicate key" in msg or "unique constraint" in msg:
            raise HTTPException(status_code=409, detail="Email dupliqué: le pattern génère des collisions (même email pour plusieurs leads).")
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {msg}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")


def PreviewEmailCollisions(
    db: Session,
    base: str,
    pattern: Optional[str] = None,
    overwrite: bool = True,
    limit_emails: int = 50,
    limit_leads_per_email: int = 20,
):
    """
    Retourne les collisions internes du pattern: emails générés identiques pour plusieurs leads.
    Utile pour diagnostiquer le 409 "duplicate email".
    """
    try:
        from sqlalchemy import text

        pattern = _normalize_email_pattern(pattern)
        limit_emails = max(1, min(int(limit_emails or 50), 200))
        limit_leads_per_email = max(1, min(int(limit_leads_per_email or 20), 200))

        # mêmes expressions que CompleteEmail
        target_email_from_societe = """
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(:pattern, '{prenom}', LOWER(REGEXP_REPLACE(sl.prenom, '\\s+', '', 'g'))),
                    '{nom}', LOWER(REGEXP_REPLACE(sl.nom, '\\s+', '', 'g'))),
                '{domaine}', s.domaine),
            '{extension}', s.extension)
        """
        target_email_from_existing = """
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(:pattern, '{prenom}', LOWER(REGEXP_REPLACE(sl.prenom, '\\s+', '', 'g'))),
                    '{nom}', LOWER(REGEXP_REPLACE(sl.nom, '\\s+', '', 'g'))),
                '{domaine}', SPLIT_PART(SPLIT_PART(sl.email, '@', 2), '.', 1)),
            '{extension}', SPLIT_PART(SPLIT_PART(sl.email, '@', 2), '.', -1))
        """

        rows = db.execute(
            text(f"""
                WITH candidates1 AS (
                    SELECT
                        sl.id,
                        sl.nom,
                        sl.prenom,
                        sl.email AS current_email,
                        sl.societe,
                        {target_email_from_societe} AS new_email
                    FROM {base} sl
                    JOIN societe_leads s ON UPPER(sl.societe) = UPPER(s.nom)
                    WHERE (
                            :overwrite = TRUE
                            OR (sl.email IS NULL OR sl.email = '' OR sl.email = 'nan')
                      )
                      AND sl.nom IS NOT NULL
                      AND sl.nom != ''
                      AND sl.nom != 'nan'
                      AND sl.prenom IS NOT NULL
                      AND sl.prenom != ''
                      AND sl.prenom != 'nan'
                      AND s.domaine IS NOT NULL
                      AND s.extension IS NOT NULL
                ),
                candidates2 AS (
                    SELECT
                        sl.id,
                        sl.nom,
                        sl.prenom,
                        sl.email AS current_email,
                        sl.societe,
                        {target_email_from_existing} AS new_email
                    FROM {base} sl
                    WHERE :overwrite = TRUE
                      AND sl.email IS NOT NULL
                      AND sl.email != ''
                      AND LOWER(sl.email) != 'nan'
                      AND sl.email LIKE '%@%.%'
                      AND sl.nom IS NOT NULL
                      AND sl.nom != ''
                      AND sl.nom != 'nan'
                      AND sl.prenom IS NOT NULL
                      AND sl.prenom != ''
                      AND sl.prenom != 'nan'
                ),
                candidates AS (
                    -- on garde un set unique (au cas où une ligne est dans les deux)
                    SELECT DISTINCT ON (id) *
                    FROM (
                        SELECT * FROM candidates1
                        UNION ALL
                        SELECT * FROM candidates2
                    ) z
                    WHERE new_email IS NOT NULL AND new_email != '' AND LOWER(new_email) != 'nan'
                    ORDER BY id
                ),
                collisions AS (
                    SELECT new_email, COUNT(*) AS cnt
                    FROM candidates
                    GROUP BY new_email
                    HAVING COUNT(*) > 1
                    ORDER BY cnt DESC, new_email
                    LIMIT :limit_emails
                ),
                ranked AS (
                    SELECT
                        c.new_email,
                        c.cnt,
                        cand.id,
                        cand.nom,
                        cand.prenom,
                        cand.current_email,
                        cand.societe,
                        ROW_NUMBER() OVER (PARTITION BY cand.new_email ORDER BY cand.id) AS rn
                    FROM collisions c
                    JOIN candidates cand ON cand.new_email = c.new_email
                )
                SELECT
                    new_email,
                    cnt,
                    id,
                    nom,
                    prenom,
                    current_email,
                    societe
                FROM ranked
                WHERE rn <= :limit_leads_per_email
                ORDER BY cnt DESC, new_email, id
            """),
            {
                "pattern": pattern,
                "overwrite": bool(overwrite),
                "limit_emails": limit_emails,
                "limit_leads_per_email": limit_leads_per_email,
            },
        ).mappings().all()

        grouped: dict[str, dict] = {}
        for r in rows:
            email = r.get("new_email") or ""
            if email not in grouped:
                grouped[email] = {"email": email, "count": int(r.get("cnt") or 0), "leads": []}
            grouped[email]["leads"].append(
                {
                    "id": r.get("id"),
                    "nom": r.get("nom"),
                    "prenom": r.get("prenom"),
                    "current_email": r.get("current_email"),
                    "societe": r.get("societe"),
                }
            )

        return {
            "pattern": pattern,
            "overwrite": bool(overwrite),
            "collisions": list(grouped.values()),
        }

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")


# Dans service/service.py
def CheckContactsBlack(db: Session, base: str):
    try:
        # Vérifier s'il reste des leads
        count_query = text(f"SELECT COUNT(*) FROM {base}")
        result = db.execute(count_query).scalar()
        
        if result == 0:
            print(f"⚠️ Aucun lead restant dans {base}, skip CheckContactsBlack")
            return {"blacklisted_removed": 0}  # ✅ Retourner un dict
        
        # Suite de votre logique existante...
        query = text(f"""
            DELETE FROM {base}
            WHERE email IN (SELECT email FROM blacklist_leads)
        """)
        res = db.execute(query)
        db.commit()
        deleted_count = res.rowcount if hasattr(res, "rowcount") else 0
        print(f"✅ {deleted_count} leads blacklistés supprimés de {base}")
        return {"blacklisted_removed": deleted_count}
        
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur CheckContactsBlack : {str(e)}")

def nettoyer_contact(db: Session):
    try:
        result = db.query(StagingLeads).all()

        if not result:
            raise HTTPException(status_code=404, detail="Aucun lead à nettoyer.")

        for i in result:
            i.nom      = NetoyerUneChaine(i.nom)
            i.prenom   = NetoyerUneChaine(i.prenom)
            i.fonction = NetoyerUneChaine(i.fonction)
            i.societe  = NetoyerUneChaine(i.societe)
            i.telephone = NetoyerUnNumero(i.telephone)
            i.email    = NettoyerUnEmail(i.email)

        db.commit() 
        print("lena")
        return {"cleaned_rows": len(result)}

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")


def StagingToProd(db: Session):
    try:
        result = db.query(StagingLeads).all()

        if not result:
            raise HTTPException(status_code=404, detail="Aucun lead en staging à traiter.")

        existing_emails = {
            r.email for r in db.query(Silver_leads.email).all()
        }

        prod_rows = []
        clean_rows = []
        duplicates = 0  

        for row in result:
            #  CAS 1 : email vide → cleaning
            if row.email is None or row.email == "":
                clean_rows.append(cleaningleads(
                    nom=row.nom,
                    prenom=row.prenom,
                    email=row.email,
                    fonction=row.fonction,
                    societe=row.societe,
                    telephone=row.telephone,
                    linkedin=row.linkedin,
                    location=row.location
                ))

            else:
                #  CAS 2 : email unique → prod
                if row.email not in existing_emails:
                    prod_rows.append(Silver_leads(
                        nom=row.nom,
                        prenom=row.prenom,
                        email=row.email,
                        fonction=row.fonction,
                        societe=row.societe,
                        telephone=row.telephone,
                        linkedin=row.linkedin,
                        location=row.location
                    ))
                    existing_emails.add(row.email)

                #  CAS 3 : email doublon → cleaning + compteur
                else:
                    duplicates += 1

        db.bulk_save_objects(prod_rows)
        db.bulk_save_objects(clean_rows)

        db.query(StagingLeads).delete()

        db.commit()

        supp = SupprimerDoublons(db, "cleaning_leads")

        return {
            "moved_to_prod": len(prod_rows),
            "moved_to_clean": abs(len(clean_rows) - supp['duplicates_deleted']),
            "duplicates_skipped": duplicates
        }

    except HTTPException:
        raise

    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {str(e)}")

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")


def SaveStatic(db: Session,static:Static):
   try:
        statics = StatisticLeads(
                filename=static.filename,
                iduser=static.iduser,
                inserted_rows=static.inserted_rows if static.inserted_rows else 0,
                emails_completed=static.emails_completed if static.emails_completed else 0,
                blacklisted_removed=static.blacklisted_removed if static.blacklisted_removed else 0,
                moved_to_silver=static.moved_to_silver if static.moved_to_silver else 0,
                moved_to_clean=static.moved_to_clean if static.moved_to_clean else 0,
                moved_to_gold=static.moved_to_gold if static.moved_to_gold else 0,
)
        print("statics",statics)
        db.add(statics)
        db.commit()
   except HTTPException:
        raise
   except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {str(e)}")
   except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")
def updatestat(db: Session, result: dict):
    print("lena")

    # Compat rétro: ajouter colonne si absente
    try:
        db.execute(text("ALTER TABLE statistic_leads ADD COLUMN IF NOT EXISTS staging_vs_applique INTEGER"))
        db.commit()
    except Exception:
        db.rollback()

    query = text("""
        UPDATE statistic_leads
        SET 
            moved_to_silver     = :moved_to_silver,
            moved_to_clean      = :moved_to_clean,
            moved_to_gold       = :moved_to_gold,

            added_societes      = :added_societes,
            emails_completed    = :emails_completed,
            societe_completed   = :societe_completed,

            total_deleted       = :total_deleted,
            staging_vs_silver   = :staging_vs_silver,
            staging_vs_gold     = :staging_vs_gold,
            staging_vs_applique = :staging_vs_applique,
            staging_internal    = :staging_internal,

            blacklisted_removed = :blacklisted_removed

        WHERE filename = :filename
    """)

    db.execute(query, {
        "moved_to_silver": result.get("moved_to_silver", 0),
        "moved_to_clean":  result.get("moved_to_clean", 0),
        "moved_to_gold":   result.get("moved_to_gold", 0),

        "added_societes": result.get("added_societes", 0),
        "emails_completed": result.get("emails_completed", 0),
        "societe_completed": result.get("societe_completed", 0),

        "total_deleted": result.get("total_deleted", 0),
        "staging_vs_silver": result.get("staging_vs_silver", 0),
        "staging_vs_gold": result.get("staging_vs_gold", 0),
        "staging_vs_applique": result.get("staging_vs_applique", 0),
        "staging_internal": result.get("staging_internal", 0),

        "blacklisted_removed": result.get("blacklisted_removed", 0),

        "filename": result.get("filename")
    })

    db.commit()


def rollback_duplicate_upload_records(db: Session, filename: str, iduser: str, inserted_rows: int):
    try:
        if not filename or not iduser or inserted_rows <= 0:
            return {"rolled_back_history": 0, "rolled_back_stats": 0}

        # Supprimer la dernière ligne statistique liée à cet import utilisateur/fichier
        stats_deleted = db.execute(text("""
            DELETE FROM statistic_leads
            WHERE id IN (
                SELECT id
                FROM statistic_leads
                WHERE filename = :filename
                  AND iduser = :iduser
                ORDER BY id DESC
                LIMIT 1
            )
        """), {"filename": filename, "iduser": iduser}).rowcount

        # Supprimer les N dernières lignes d'historique liées à cet import
        history_deleted = db.execute(text("""
            DELETE FROM staging_import_history
            WHERE id IN (
                SELECT id
                FROM staging_import_history
                WHERE filename = :filename
                  AND iduser = :iduser
                ORDER BY id DESC
                LIMIT :lim
            )
        """), {"filename": filename, "iduser": iduser, "lim": inserted_rows}).rowcount

        db.commit()
        return {"rolled_back_history": history_deleted, "rolled_back_stats": stats_deleted}
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur rollback import doublon : {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue rollback import doublon : {str(e)}")
