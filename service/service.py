from fastapi import UploadFile, HTTPException
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
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
def normalize_col(col: str) -> str:
    result = unicodedata.normalize('NFKD', str(col)).encode('ascii', 'ignore').decode('ascii').strip().lower()
    result = result.replace("'", " ").replace("'", " ").replace("`", " ")
    return result
def LoadFileToBd(file: UploadFile, db: Session):
    COLUMN_ALIASES = {
        "Nom":       ["nom", "last name", "lastname", "last_name", "surname", "nom du contact"],
        "Prenom":    ["prenom", "first name", "firstname", "first_name", "name", "prenom du contact"],
        "Email":     ["email", "mail", "e-mail", "courriel", "adresse mail"],
        "Fonction":  ["fonction", "title", "titre", "poste", "job title"],
        "Societe":   ["societe", "company", "company name", "entreprise", "organization", "nom de l entreprise"],
        "Telephone": ["telephone", "tel", "phone", "mobile", "gsm"],
        "Linkedin":  ["linkedin", "cnx et msg linkedin"],
    }
 
    try:
        file.file.seek(0)
 
        # 📄 CSV
        if file.filename.endswith(".csv"):
            raw = file.file.read()
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError:
                text = raw.decode("latin1")
 
            sep = ","
            for delimiter in [",", ";", "\t", "|"]:
                try:
                    test_df = pd.read_csv(io.StringIO(text), sep=delimiter, nrows=5, dtype=str)
                    if len(test_df.columns) > 2:
                        sep = delimiter
                        break
                except:
                    continue
 
            df = pd.read_csv(io.StringIO(text), sep=sep, engine="python", dtype=str)
 
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
        rename_map = {}
        for standard_name, aliases in COLUMN_ALIASES.items():
            for col in df.columns:
                if col.strip() in aliases and standard_name not in rename_map.values():
                    rename_map[col] = standard_name
                    break
 
        df.rename(columns=rename_map, inplace=True)
        print(f"📋 Colonnes après rename: {df.columns.tolist()}")
        
        # ✅ SUPPRIMER LES COLONNES DUPLIQUÉES (garder la première occurrence)
        df = df.loc[:, ~df.columns.duplicated(keep='first')]
        print(f"🔧 Colonnes après suppression des doublons: {df.columns.tolist()}")
 
        # ➕ Ajouter colonnes manquantes
        for col in ["Nom", "Prenom", "Email", "Fonction", "Societe", "Telephone", "Linkedin"]:
            if col not in df.columns:
                df[col] = None
 
        # ✅ SÉLECTIONNER UNIQUEMENT LES 7 COLONNES NÉCESSAIRES
        df_clean = df[["Nom", "Prenom", "Email", "Fonction", "Societe", "Telephone", "Linkedin"]].copy()
        
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
        df_clean.columns = ["nom", "prenom", "email", "fonction", "societe", "telephone", "linkedin"]

        # 🧹 NETTOYAGE AVANT INSERTION
        print("🧹 Nettoyage des données...")
        
        df_clean['nom'] = df_clean['nom'].apply(NetoyerUneChaine)
        df_clean['prenom'] = df_clean['prenom'].apply(NetoyerUneChaine)
        df_clean['fonction'] = df_clean['fonction'].apply(NetoyerUneChaine)
        df_clean['societe'] = df_clean['societe'].apply(NetoyerUneChaine)
        df_clean['telephone'] = df_clean['telephone'].apply(NetoyerUnNumero)
        df_clean['email'] = df_clean['email'].apply(NettoyerUnEmail)

        print("✅ Nettoyage terminé")

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
        
        print(f"✅ {len(df_clean)} lignes insérées et nettoyées avec succès")
        
        return {"inserted_rows": len(df_clean)}

 
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur DB : {str(e)}")
 
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur : {str(e)}")

def SupprimerDoublons(db: Session, table: str):
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


def CompleteEmail(db: Session):
    print("🔄 Début de complétion des emails...")
    try:
        from sqlalchemy import text
        
        # Vérifier qu'il y a des sociétés
        count_societes = db.query(societeleads).count()
        if count_societes == 0:
            raise HTTPException(status_code=404, detail="Aucune société trouvée en base.")

        # UPDATE avec JOIN - UNIQUEMENT si email est NULL ou vide
        result = db.execute(text("""
            UPDATE staging_leads sl
            SET email = CONCAT(sl.prenom, '.', sl.nom, '@', s.domaine, '.', s.extension)
            FROM societe_leads s
            WHERE UPPER(sl.societe) = UPPER(s.nom)
              AND (sl.email IS NULL OR sl.email = '' OR sl.email = 'nan')
              AND sl.nom IS NOT NULL 
              AND sl.nom != ''
              AND sl.nom != 'nan'
              AND sl.prenom IS NOT NULL 
              AND sl.prenom != ''
              AND sl.prenom != 'nan'
              AND s.domaine IS NOT NULL
              AND s.extension IS NOT NULL
        """))
        
        db.commit()
        emails_completed = result.rowcount
        
        print(f"✅ {emails_completed} emails complétés")
        return {"emails_completed": emails_completed}

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")


def CheckContactsBlack(db: Session):
    try:
        from sqlalchemy import text
        
        # 1️⃣ Vérifier s'il y a des emails blacklistés
        count_blacklist = db.query(blacklistLeads).count()
        
        if count_blacklist == 0:
            return {"blacklisted_removed": 0}

        # 2️⃣ Compter les lignes avant suppression
        total_before = db.query(StagingLeads).count()
        
        if total_before == 0:
            raise HTTPException(status_code=404, detail="Aucun lead en staging.")

        # 3️⃣ DELETE avec JOIN en SQL pur (ultra-rapide)
        result = db.execute(text("""
            DELETE FROM staging_leads
            WHERE email IN (
                SELECT email FROM blacklist_leads
            )
        """))
        
        db.commit()
        blacklisted_removed = result.rowcount
        
        # 4️⃣ Compter les lignes restantes
        total_after = db.query(StagingLeads).count()
        
        print(f"✅ {blacklisted_removed} contacts blacklistés supprimés")
        print(f"📊 Lignes restantes: {total_after}")
        
        return {
            "blacklisted_removed": blacklisted_removed,
            "lignes_restantes": total_after
        }

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")

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
                    linkedin=row.linkedin
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
                        linkedin=row.linkedin
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
                inserted_rows=static.inserted_rows if static.inserted_rows else 0,
                duplicates_deleted=static.duplicates_deleted if static.duplicates_deleted else 0,
                emails_completed=static.emails_completed if static.emails_completed else 0,
                blacklisted_removed=static.blacklisted_removed if static.blacklisted_removed else 0,
                moved_to_silver=static.moved_to_silver if static.moved_to_silver else 0,
                moved_to_clean=static.moved_to_clean if static.moved_to_clean else 0,
                moved_to_gold=static.moved_to_gold if static.moved_to_gold else 0,
)
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
def updatestat(db:Session,result:dict):
        print("lena")
        query = text("""
            UPDATE statistic_leads
            SET 
                moved_to_silver = :moved_to_silver,
                moved_to_clean  = :moved_to_clean,
                moved_to_gold   = :moved_to_gold
            WHERE filename = :filename
        """)
        db.execute(query, {
            "moved_to_silver": result["moved_to_silver"],
            "moved_to_clean":  result["moved_to_clean"],
            "moved_to_gold":   result["moved_to_gold"],
            "filename":        result["filename"],
        })
        db.commit()

