from fastapi import UploadFile, HTTPException
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from model.staging_leads import StagingLeads
from model.blacklistLeads import blacklistLeads
from model.cleaning_leads import cleaningleads
from model.societe_leads import societeleads
from model.prod_leads import Prod_leads
from util.util import NetoyerUneChaine, NetoyerUnNumero, NettoyerUnEmail
from model.statistiqueLeads import StatisticLeads
from schema.schemaStatic import Static 

def LoadFileToBd(file: UploadFile, db: Session):
    COLUMN_ALIASES = {
        "Nom":       ["nom", "last name", "lastname", "last_name", "surname"],
        "Prenom":    ["prenom", "prénom", "first name", "firstname", "first_name", "name"],
        "Email":     ["email", "mail", "e-mail", "courriel"],
        "Fonction":  ["fonction", "title", "titre", "poste", "job title", "action"],
        "Societe":   ["societe", "société", "company", "company name", "entreprise", "organization"],
        "Telephone": ["telephone", "téléphone", "tel", "phone", "mobile", "gsm"],
        "Linkedin":  ["linkedin"],
    }

    try:
        if not file.filename:
            raise HTTPException(status_code=400, detail="Nom de fichier manquant.")

        if not (file.filename.endswith(".csv") or file.filename.endswith(".xlsx") or file.filename.endswith(".xls")):
            raise HTTPException(status_code=400, detail="Format de fichier non supporté. Utilisez .csv ou .xlsx")

        try:
            file.file.seek(0)
            if ".csv" in file.filename:
                # ✅ détecte automatiquement le séparateur
                sample = file.file.read(4096).decode("latin1", errors="ignore")
                file.file.seek(0)
                import csv
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=",;|\t")
                    sep = dialect.delimiter
                except Exception:
                    sep = ","
                df = pd.read_csv(
                    file.file,
                    encoding="latin1",
                    sep=sep,
                    on_bad_lines="skip",   # ✅ ignore les lignes malformées
                    engine="python",       # ✅ plus tolérant
                    quoting=csv.QUOTE_ALL, # ✅ gère les virgules dans les cellules
                )
            else:
                df = pd.read_excel(file.file)
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Impossible de lire le fichier : {str(e)}")

        # ✅ nettoie les noms de colonnes
        df.columns = df.columns.str.strip().str.replace(r"\s+", " ", regex=True)

        # ✅ renommage insensible à la casse
        rename_map = {}
        for standard_name, aliases in COLUMN_ALIASES.items():
            for col in df.columns:
                if col.strip().lower() in aliases and standard_name not in rename_map.values():
                    rename_map[col] = standard_name
                    break
        df.rename(columns=rename_map, inplace=True)

        # ✅ ajoute les colonnes manquantes
        all_columns = ["Nom", "Prenom", "Email", "Fonction", "Societe", "Telephone", "Linkedin"]
        for col in all_columns:
            if col not in df.columns:
                df[col] = None

        # ✅ supprime les lignes complètement vides
        df.dropna(how="all", inplace=True)

        if df.empty:
            raise HTTPException(status_code=400, detail="Le fichier est vide.")

        def clean(val):
            if val is None:
                return None
            s = str(val).strip()
            return None if s.lower() in ("nan", "none", "", "n/a", "null") else s

        users = []
        for i in df.itertuples():
            users.append(
                StagingLeads(
                    nom=clean(i.Nom),
                    prenom=clean(i.Prenom),
                    email=clean(i.Email),
                    fonction=clean(i.Fonction),
                    societe=clean(i.Societe),
                    telephone=clean(i.Telephone),
                    linkedin=clean(i.Linkedin),
                )
            )

        db.add_all(users)
        db.commit()
        return {"inserted_rows": len(users)}

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")


def SupprimerDoublons(db: Session, table: str):
    try:
        allowed_tables = {"staging_leads", "cleaning_leads", "prod_leads"}
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
                    COALESCE(societe, ''),
                    COALESCE(telephone, ''),
                    COALESCE(linkedin, '')
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
    try:
        societes = {s.nom.upper(): s for s in db.query(societeleads).all()}

        if not societes:
            raise HTTPException(status_code=404, detail="Aucune société trouvée en base.")

        result = db.query(StagingLeads).filter(
            StagingLeads.email == None,
            StagingLeads.nom != None,
            StagingLeads.prenom != None
        ).all()

        emails_completed = 0
        for user in result:
            if user.societe:
                societe = societes.get(user.societe.upper())
                if societe:
                    user.email = f"{user.prenom}.{user.nom}@{societe.domaine}.{societe.extension}"
                    emails_completed += 1

        db.commit()
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
        blacklisted_emails = {
            b.email for b in db.query(blacklistLeads.email).all()
        }

        if not blacklisted_emails:
            return {"blacklisted_removed": 0}

        leads = db.query(StagingLeads).all()

        if not leads:
            raise HTTPException(status_code=404, detail="Aucun lead en staging.")

        blacklisted_removed = 0
        for user in leads:
            if user.email and user.email in blacklisted_emails:
                db.delete(user)
                blacklisted_removed += 1

        db.commit()
        return {"blacklisted_removed": blacklisted_removed,"liggne rester":len(leads)}

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
            raise HTTPException(status_code=404, detail="Aucun lead à nettoyer en staging.")

        for i in result:
            i.nom = NetoyerUneChaine(i.nom)
            i.prenom = NetoyerUneChaine(i.prenom)
            i.fonction = NetoyerUneChaine(i.fonction)
            i.societe = NetoyerUneChaine(i.societe)
            i.telephone = NetoyerUnNumero(i.telephone)
            i.email = NettoyerUnEmail(i.email)

        db.add_all(result)
        db.commit()
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
            r.email for r in db.query(Prod_leads.email).all()
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
                    prod_rows.append(Prod_leads(
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
            "moved_to_clean": len(clean_rows) - supp['duplicates_deleted'],
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
        statics=StatisticLeads(
            filename=static.filename,
            inserted_rows = static.inserted_rows,
            duplicates_deleted=static.duplicates_deleted,
            emails_completed=static.emails_completed,
            blacklisted_removed=static.blacklisted_removed,
            moved_to_prod =static.moved_to_clean,
            moved_to_clean =static.moved_to_prod
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

