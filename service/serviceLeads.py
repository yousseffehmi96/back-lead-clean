from sqlalchemy.orm import Session
from model.societe_leads import societeleads
from fastapi import HTTPException,Depends
from model.silver_leads import Silver_leads
from model.gold_leads import Gold_leads
from model.blacklistLeads import blacklistLeads
from fastapi.responses import StreamingResponse
from model.cleaning_leads import cleaningleads
from model.statistiqueLeads import StatisticLeads
from model.staging_leads import StagingLeads
from model.staging_import_history import StagingImportHistory
from model.steaging_applique import SteagingApplique
from model.gold_leads import Gold_leads
from sqlalchemy import or_,and_
from sqlalchemy import text
from datetime import datetime
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

import csv
import io
import zipfile
from sqlalchemy.exc import SQLAlchemyError
from service import service as se
from database.db import get_db
import unicodedata
from util.util import NettoyerUnEmail
def GetAllSilver(db:Session):
    return db.query(Silver_leads).all()
def GetAllGold(db:Session):
    return db.query(Gold_leads).all()
def GetAllBlack(db:Session):
    return db.query(blacklistLeads).all()
def GetAllClean(db:Session):
    return db.query(cleaningleads).all()
def GetAllStat(db: Session, userid: str | None = None, is_manager: bool = False):
    q = db.query(StatisticLeads)
    if not is_manager:
        if not userid:
            return []
        q = q.filter(StatisticLeads.iduser == userid)
    return q.order_by(StatisticLeads.created_at.desc()).all()
def GetAllStaging(db:Session):
    return db.query(StagingLeads).all()
def GetAllSteagingApplique(db: Session):
    return db.query(SteagingApplique).all()

def ExportDatabaseZip(db: Session, is_manager: bool):
    if not is_manager:
        raise HTTPException(status_code=403, detail="Accès refusé: manager seulement")

    # Assurer colonnes export utiles (compat rétro)
    try:
        db.execute(text("ALTER TABLE staging_import_history ADD COLUMN IF NOT EXISTS username TEXT"))
        db.commit()
    except Exception:
        db.rollback()

    # Liste blanche des tables à exporter
    tables = [
        "societe_leads",
        "staging_leads",
        "staging_import_history",
        "cleaning_leads",
        "silver_leads",
        "gold_leads",
        "blacklist_leads",
        "steaging_applique",
        "statistic_leads",
        "validation_rule",
        "token",
        "app_settings",
    ]

    def to_str(v):
        if v is None:
            return ""
        return str(v)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for table in tables:
            try:
                # Forcer l'ordre des colonnes pour staging_import_history afin de rendre "username" visible.
                if table == "staging_import_history":
                    rows = db.execute(text("""
                        SELECT
                            id,
                            filename,
                            COALESCE(username, '') AS username,
                            nom,
                            prenom,
                            email,
                            fonction,
                            societe,
                            telephone,
                            linkedin,
                            location,
                            imported_at
                        FROM staging_import_history
                    """)).mappings().all()
                else:
                    rows = db.execute(text(f"SELECT * FROM {table}")).mappings().all()
            except Exception:
                # table non existante dans certains environnements
                continue

            # Si table vide: ne pas exporter de fichier
            if not rows:
                continue

            csv_buf = io.StringIO()
            if rows:
                headers = list(rows[0].keys())
                writer = csv.DictWriter(csv_buf, fieldnames=headers, delimiter=";", lineterminator="\n")
                writer.writeheader()
                for r in rows:
                    writer.writerow({k: to_str(r.get(k)) for k in headers})

            zf.writestr(f"{table}.csv", csv_buf.getvalue().encode("utf-8-sig"))

    buf.seek(0)
    filename = f"db_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
    return StreamingResponse(
        buf,
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

def CountLastImportAlreadyProcessedInApplique(db: Session, filename: str, userid: str, inserted_rows: int) -> int:
    """
    Compte combien de lignes du dernier import (history) existent déjà dans steaging_applique.
    Matching prioritaire par email (non vide), sinon par (nom, prenom, societe).
    """
    if not filename or not userid or inserted_rows <= 0:
        return 0
    try:
        res = db.execute(text("""
            WITH last_import AS (
                SELECT id, nom, prenom, email, societe
                FROM staging_import_history
                WHERE filename = :filename
                  AND iduser = :userid
                ORDER BY id DESC
                LIMIT :lim
            ),
            normalized AS (
                SELECT
                    id,
                    LOWER(TRIM(COALESCE(nom, ''))) AS nom_n,
                    LOWER(TRIM(COALESCE(prenom, ''))) AS prenom_n,
                    LOWER(TRIM(COALESCE(email, ''))) AS email_n,
                    LOWER(TRIM(COALESCE(societe, ''))) AS societe_n
                FROM last_import
            )
            SELECT COUNT(*)
            FROM normalized li
            WHERE (
                (li.email_n <> '' AND li.email_n <> 'nan' AND EXISTS (
                    SELECT 1 FROM steaging_applique sa
                    WHERE LOWER(TRIM(COALESCE(sa.email, ''))) = li.email_n
                ))
                OR
                ((li.email_n = '' OR li.email_n = 'nan') AND EXISTS (
                    SELECT 1 FROM steaging_applique sa
                    WHERE LOWER(TRIM(COALESCE(sa.nom, ''))) = li.nom_n
                      AND LOWER(TRIM(COALESCE(sa.prenom, ''))) = li.prenom_n
                      AND LOWER(TRIM(COALESCE(sa.societe, ''))) = li.societe_n
                ))
            )
        """), {"filename": filename, "userid": userid, "lim": int(inserted_rows)}).scalar()
        return int(res or 0)
    except Exception:
        # Ne pas casser l'upload si ce check échoue
        return 0

def UpdateSilverEmail(db: Session, lead_id: int, email: str):
    try:
        cleaned = NettoyerUnEmail(email)
        if not cleaned:
            raise HTTPException(status_code=400, detail="Email invalide")

        lead = db.query(Silver_leads).filter(Silver_leads.id == lead_id).first()
        if not lead:
            raise HTTPException(status_code=404, detail="Lead silver introuvable")

        lead.email = cleaned
        db.commit()
        db.refresh(lead)
        return {"message": "Email mis à jour", "email": lead.email}
    except HTTPException:
        raise
    except SQLAlchemyError as e:
        db.rollback()
        # cas typique: contrainte unique sur email
        msg = str(e)
        if "UniqueViolation" in msg or "duplicate key" in msg or "unique constraint" in msg:
            raise HTTPException(status_code=409, detail="Email déjà utilisé")
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {msg}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")
def ClearBaseTable(db: Session, base: str):
    try:
        result = db.execute(text(f"DELETE FROM {base}"))
        db.commit()
        return {"staging_cleared_rows": result.rowcount}
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")
def GetAllStagingImportHistory(db: Session, userid: str | None = None):
    def norm(v):
        s = (v or "").strip().lower()
        return "" if s == "nan" else s

    # Filtre user côté backend pour réduire fortement le volume.
    history_query = db.query(StagingImportHistory)
    if userid:
        history_query = history_query.filter(StagingImportHistory.iduser == userid)
    history_rows = history_query.order_by(StagingImportHistory.imported_at.desc()).all()

    # Sets de matching en mémoire (beaucoup plus rapide que CASE/EXISTS corrélé).
    gold_emails = {norm(x[0]) for x in db.query(Gold_leads.email).all() if norm(x[0])}
    silver_emails = {norm(x[0]) for x in db.query(Silver_leads.email).all() if norm(x[0])}
    clean_emails = {norm(x[0]) for x in db.query(cleaningleads.email).all() if norm(x[0])}
    blacklist_emails = {norm(x[0]) for x in db.query(blacklistLeads.email).all() if norm(x[0])}

    gold_keys = {(norm(r.nom), norm(r.prenom), norm(r.societe)) for r in db.query(Gold_leads.nom, Gold_leads.prenom, Gold_leads.societe).all()}
    silver_keys = {(norm(r.nom), norm(r.prenom), norm(r.societe)) for r in db.query(Silver_leads.nom, Silver_leads.prenom, Silver_leads.societe).all()}
    clean_keys = {(norm(r.nom), norm(r.prenom), norm(r.societe)) for r in db.query(cleaningleads.nom, cleaningleads.prenom, cleaningleads.societe).all()}

    enriched = []
    for h in history_rows:
        email_key = norm(h.email)
        tuple_key = (norm(h.nom), norm(h.prenom), norm(h.societe))

        # Règle: éliminer les leads blacklisted de l'historique
        if email_key and email_key in blacklist_emails:
            continue

        if email_key:
            if email_key in gold_emails:
                destination = "gold"
            elif email_key in silver_emails:
                destination = "silver"
            elif email_key in clean_emails:
                destination = "clean"
            else:
                destination = "staging"
        else:
            if tuple_key in gold_keys:
                destination = "gold"
            elif tuple_key in silver_keys:
                destination = "silver"
            elif tuple_key in clean_keys:
                destination = "clean"
            else:
                destination = "staging"

        enriched.append({
            "id": h.id,
            "filename": h.filename,
            "iduser": h.iduser,
            "nom": h.nom,
            "prenom": h.prenom,
            "email": h.email,
            "fonction": h.fonction,
            "societe": h.societe,
            "telephone": h.telephone,
            "linkedin": h.linkedin,
            "location": h.location,
            "imported_at": h.imported_at,
            "destination": destination,
        })

    return enriched
def DownloadProdLeadCSV(types:str,db: Session):
    try:
        # 1️⃣ Charger les données
        if(types=="silver"):
            leads = db.query(Silver_leads).all()
        else:   
            leads = db.query(Gold_leads).all()

        
        if not leads:
            raise HTTPException(status_code=404, detail="Aucun lead à télécharger")

        # 2️⃣ Créer le buffer avec BOM UTF-8 (pour que Excel reconnaisse l'UTF-8)
        output = io.StringIO()
        output.write('\ufeff')  # BOM pour UTF-8
        
        # 3️⃣ Créer le writer CSV
        writer = csv.writer(
            output, 
            delimiter=';',           # Point-virgule pour Excel français
            quoting=csv.QUOTE_MINIMAL,
            lineterminator='\n'
        )
        
        # 4️⃣ En-têtes
        writer.writerow([
            "Nom",
            "Prénom",
            "Email",
            "Fonction",
            "Société",
            "Téléphone",
            "LinkedIn",
            "Location"
        ])
        
        # 5️⃣ Données (gérer les None)
        for lead in leads:
            writer.writerow([
                lead.nom or "",
                lead.prenom or "",
                lead.email or "",
                lead.fonction or "",
                lead.societe or "",
                lead.telephone or "",
                lead.linkedin or "",
                lead.location or ""
            ])
        
        output.seek(0)
        
        # 6️⃣ Nom du fichier avec timestamp
        filename = f"leads_silver_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        # 7️⃣ Retourner le fichier
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Type": "text/csv; charset=utf-8"
            }
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors de la génération du CSV : {str(e)}")
def DownloadLeadXlsx(types:str,db: Session):
    try:
        if(types=="silver"):
            leads = db.query(Silver_leads).all()
        else:   
            leads = db.query(Gold_leads).all()
        
        if not leads:
            raise HTTPException(status_code=404, detail="Aucun lead à télécharger")

        # 2️⃣ Créer le workbook
        wb = Workbook()
        sheet = wb.active
        sheet.title = "Leads Silver"
        
        # 3️⃣ En-têtes
        headers = ["Nom", "Prénom", "Email", "Fonction", "Société", "Téléphone", "LinkedIn", "Location"]
        sheet.append(headers)
        
        # 4️⃣ Style des en-têtes
        header_font = Font(bold=True, color="FFFFFF", size=12)
        header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
        header_alignment = Alignment(horizontal="center", vertical="center")
        
        for col_num, header in enumerate(headers, 1):
            cell = sheet.cell(row=1, column=col_num)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = header_alignment
        
        # 5️⃣ Ajouter les données
        for lead in leads:
            sheet.append([
                lead.nom,
                lead.prenom,
                lead.email,
                lead.fonction,
                lead.societe,
                lead.telephone,
                lead.linkedin,
                lead.location
            ])
        
        # 6️⃣ Auto-ajuster la largeur des colonnes
        for col_num, header in enumerate(headers, 1):
            column_letter = get_column_letter(col_num)
            max_length = len(header)
            
            for row in sheet.iter_rows(min_row=2, max_row=sheet.max_row, min_col=col_num, max_col=col_num):
                for cell in row:
                    if cell.value:
                        max_length = max(max_length, len(str(cell.value)))
            
            # Largeur avec marge
            adjusted_width = min(max_length + 2, 50)
            sheet.column_dimensions[column_letter].width = adjusted_width
        
        # 7️⃣ Bordures pour toutes les cellules
        thin_border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        
        for row in sheet.iter_rows(min_row=1, max_row=sheet.max_row, min_col=1, max_col=len(headers)):
            for cell in row:
                cell.border = thin_border
                if cell.row > 1:  # Données (pas en-têtes)
                    cell.alignment = Alignment(vertical="center")
        
        # 8️⃣ Figer la première ligne
        sheet.freeze_panes = "A2"
        
        # 9️⃣ Sauvegarder dans un buffer
        output = io.BytesIO()
        wb.save(output)
        output.seek(0)
        
        # 🔟 Retourner le fichier
        filename = f"leads_silver_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors de la génération du fichier : {str(e)}")

def _get_latest_import_rows_for_user(db: Session, userid: str):
    latest = (
        db.query(StagingImportHistory.filename)
        .filter(StagingImportHistory.iduser == userid)
        .order_by(StagingImportHistory.imported_at.desc())
        .first()
    )
    if not latest or not latest[0]:
        return None, []

    latest_filename = latest[0]
    rows = (
        db.query(StagingImportHistory)
        .filter(
            StagingImportHistory.iduser == userid,
            StagingImportHistory.filename == latest_filename
        )
        .order_by(StagingImportHistory.imported_at.desc(), StagingImportHistory.id.desc())
        .all()
    )

    # Règle export: éliminer les leads blacklisted
    blacklist_emails = {(e or "").strip().lower() for (e,) in db.query(blacklistLeads.email).all() if (e or "").strip().lower() not in ("", "nan")}
    if blacklist_emails:
        rows = [r for r in rows if ((r.email or "").strip().lower() not in blacklist_emails)]
    return latest_filename, rows

def DownloadLatestStagingImportCSV(db: Session, userid: str):
    try:
        latest_filename, rows = _get_latest_import_rows_for_user(db, userid)
        if not rows:
            raise HTTPException(status_code=404, detail="Aucun import trouvé pour cet utilisateur")

        output = io.StringIO()
        output.write('\ufeff')
        writer = csv.writer(
            output,
            delimiter=';',
            quoting=csv.QUOTE_MINIMAL,
            lineterminator='\n'
        )

        writer.writerow(["Nom", "Prénom", "Email", "Fonction", "Société", "Téléphone", "LinkedIn", "Location"])
        for lead in rows:
            writer.writerow([
                lead.nom or "",
                lead.prenom or "",
                lead.email or "",
                lead.fonction or "",
                lead.societe or "",
                lead.telephone or "",
                lead.linkedin or "",
                lead.location or "",
            ])

        output.seek(0)
        filename = f"staging_last_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "X-Import-Filename": latest_filename,
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur export dernier import CSV : {str(e)}")

def DownloadLatestStagingImportXlsx(db: Session, userid: str):
    try:
        latest_filename, rows = _get_latest_import_rows_for_user(db, userid)
        if not rows:
            raise HTTPException(status_code=404, detail="Aucun import trouvé pour cet utilisateur")

        wb = Workbook()
        sheet = wb.active
        sheet.title = "Dernier import staging"

        headers = ["Nom", "Prénom", "Email", "Fonction", "Société", "Téléphone", "LinkedIn", "Location"]
        sheet.append(headers)

        header_font = Font(bold=True, color="FFFFFF", size=12)
        header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
        header_alignment = Alignment(horizontal="center", vertical="center")

        for col_num, header in enumerate(headers, 1):
            cell = sheet.cell(row=1, column=col_num)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = header_alignment

        for lead in rows:
            sheet.append([
                lead.nom or "",
                lead.prenom or "",
                lead.email or "",
                lead.fonction or "",
                lead.societe or "",
                lead.telephone or "",
                lead.linkedin or "",
                lead.location or "",
            ])

        for col_num, header in enumerate(headers, 1):
            column_letter = get_column_letter(col_num)
            max_length = len(header)
            for row in sheet.iter_rows(min_row=2, max_row=sheet.max_row, min_col=col_num, max_col=col_num):
                for cell in row:
                    if cell.value:
                        max_length = max(max_length, len(str(cell.value)))
            sheet.column_dimensions[column_letter].width = min(max_length + 2, 50)

        thin_border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        for row in sheet.iter_rows(min_row=1, max_row=sheet.max_row, min_col=1, max_col=len(headers)):
            for cell in row:
                cell.border = thin_border
                if cell.row > 1:
                    cell.alignment = Alignment(vertical="center")

        sheet.freeze_panes = "A2"

        output = io.BytesIO()
        wb.save(output)
        output.seek(0)

        filename = f"staging_last_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "X-Import-Filename": latest_filename,
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur export dernier import XLSX : {str(e)}")
def ToBlack(id:int,eliminer:str,db:Session):
    result =db.query(Gold_leads).filter(Gold_leads.id==id).first()
    if (not result):
        raise HTTPException(
               status_code=404,
               detail='Leads non trouvè'
        )
    print(result.nom)
    blocklead=blacklistLeads(
                    id=result.id,
                    nom=result.nom,
                    prenom= result.prenom,
                    email=result.email,
                    fonction= result.fonction,
                    societe= result.societe,
                    telephone=result.telephone,
                    linkedin= result.linkedin,
                    location=result.location,
                    eliminer=eliminer
                )
    db.add(blocklead)
    db.delete(result)
    db.commit()
    return {
            "message": "Le leads a èté blocque avec succeè"
        }
    
def StagingToSilver(db: Session,base:str):
    try:
        # 1️⃣ INSERT INTO silver_leads depuis staging (évite les doublons)
        result = db.execute(text(f"""
            INSERT INTO silver_leads (nom, prenom, email, fonction, societe, telephone, linkedin, location)
            SELECT DISTINCT ON (email) 
                nom, prenom, email, fonction, societe, telephone, linkedin, location
            FROM {base}
            WHERE email IS NOT NULL 
              AND email != '' 
              AND email != 'nan'
              AND nom IS NOT NULL 
              AND nom != '' 
              AND nom != 'nan'
              AND prenom IS NOT NULL 
              AND prenom != '' 
              AND prenom != 'nan'
              AND societe IS NOT NULL 
              AND societe != '' 
              AND societe != 'nan'
              AND (
                  fonction IS NULL OR fonction = '' OR fonction = 'nan'
                  OR telephone IS NULL OR telephone = '' OR telephone = 'nan'
                  OR linkedin IS NULL OR linkedin = '' OR linkedin = 'nan'
              )
              AND NOT EXISTS (
                  SELECT 1 FROM silver_leads s WHERE s.email = {base}.email
              )
            ORDER BY email, id
        """))
        
        moved_count = result.rowcount
        
        # 2️⃣ DELETE depuis staging (ceux qui ont été déplacés + doublons internes)
        db.execute(text(f"""
            DELETE FROM {base}
            WHERE email IS NOT NULL 
              AND email != '' 
              AND email != 'nan'
              AND nom IS NOT NULL 
              AND nom != '' 
              AND nom != 'nan'
              AND prenom IS NOT NULL 
              AND prenom != '' 
              AND prenom != 'nan'
              AND societe IS NOT NULL 
              AND societe != '' 
              AND societe != 'nan'
              AND (
                  fonction IS NULL OR fonction = '' OR fonction = 'nan'
                  OR telephone IS NULL OR telephone = '' OR telephone = 'nan'
                  OR linkedin IS NULL OR linkedin = '' OR linkedin = 'nan'
              )
        """))
        
        db.commit()
        
        print(f"✅ {moved_count} leads déplacés vers Silver")
        return {"moved_to_silver": moved_count}

    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")
def StagingToClean(db: Session):
    try:
        # 1️⃣ Inserer les leads dont nom ou prenom reste vide
        # (la complétion depuis email est déjà tentée avant cet appel)
        result = db.execute(text("""
            INSERT INTO cleaning_leads (nom, prenom, email, fonction, societe, telephone, linkedin, location)
            SELECT
                nom, prenom, email, fonction, societe, telephone, linkedin, location
            FROM staging_leads sl
            WHERE (
                    LOWER(TRIM(COALESCE(sl.nom, ''))) IN ('', 'nan')
                    OR LOWER(TRIM(COALESCE(sl.prenom, ''))) IN ('', 'nan')
                  )
              AND NOT EXISTS (
                   SELECT 1
                   FROM cleaning_leads cl
                   WHERE COALESCE(cl.nom, '') = COALESCE(sl.nom, '')
                     AND COALESCE(cl.prenom, '') = COALESCE(sl.prenom, '')
                     AND COALESCE(cl.email, '') = COALESCE(sl.email, '')
                     AND COALESCE(cl.fonction, '') = COALESCE(sl.fonction, '')
                     AND COALESCE(cl.societe, '') = COALESCE(sl.societe, '')
                     AND COALESCE(cl.telephone, '') = COALESCE(sl.telephone, '')
                     AND COALESCE(cl.linkedin, '') = COALESCE(sl.linkedin, '')
                     AND COALESCE(cl.location, '') = COALESCE(sl.location, '')
              )
        """))
        
        moved_count = result.rowcount
        
        # Supprimer uniquement les leads de staging qui matchent la règle clean
        db.execute(text("""
            DELETE FROM staging_leads
            WHERE (
                    LOWER(TRIM(COALESCE(nom, ''))) IN ('', 'nan')
                    OR LOWER(TRIM(COALESCE(prenom, ''))) IN ('', 'nan')
                  )
        """))
        clean = se.SupprimerDoublonsMemetABLE(db, "cleaning_leads")
        net_moved = moved_count - clean["duplicates_deleted"]
        db.commit()

        
        print(f"✅ {net_moved} leads déplacés vers Cleaning")
        return {"moved_to_clean": net_moved}

    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")

def StagingToSteagingApplique(db: Session, base: str):
    try:
        result = db.execute(text(f"""
            INSERT INTO steaging_applique (nom, prenom, email, fonction, societe, telephone, linkedin, location)
            SELECT nom, prenom, email, fonction, societe, telephone, linkedin, location
            FROM {base}
        """))

        moved_count = result.rowcount

        db.execute(text(f"DELETE FROM {base}"))
        db.commit()

        return {"moved_to_steaging_applique": moved_count}
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")

def StagingToGold(db: Session,base:str):
    try:
        
        #  INSERT INTO gold_leads depuis staging (évite les doublons)
        result = db.execute(text(f"""
            INSERT INTO gold_leads (nom, prenom, email, fonction, societe, telephone, linkedin, location)
            SELECT DISTINCT ON (email) 
                nom, prenom, email, fonction, societe, telephone, linkedin, location
            FROM {base}
            WHERE email IS NOT NULL 
              AND email != '' 
              AND email != 'nan'
              AND nom IS NOT NULL 
              AND nom != '' 
              AND nom != 'nan'
              AND prenom IS NOT NULL 
              AND prenom != '' 
              AND prenom != 'nan'
              AND societe IS NOT NULL 
              AND societe != '' 
              AND societe != 'nan'
              AND fonction IS NOT NULL 
              AND fonction != '' 
              AND fonction != 'nan'
              AND telephone IS NOT NULL 
              AND telephone != '' 
              AND telephone != 'nan'
              AND linkedin IS NOT NULL 
              AND linkedin != '' 
              AND linkedin != 'nan'
              AND location IS NOT NULL
              AND location != ''
              AND location != 'nan'
              AND NOT EXISTS (
                  SELECT 1 FROM gold_leads g WHERE g.email = {base}.email
              )
            ORDER BY email, id
        """))
        print(result)
        moved_count = result.rowcount
        
        #  DELETE depuis staging (ceux qui ont été déplacés + doublons internes)
        db.execute(text(f"""
            DELETE FROM {base}
            WHERE email IS NOT NULL 
              AND email != '' 
              AND email != 'nan'
              AND nom IS NOT NULL 
              AND nom != '' 
              AND nom != 'nan'
              AND prenom IS NOT NULL 
              AND prenom != '' 
              AND prenom != 'nan'
              AND societe IS NOT NULL 
              AND societe != '' 
              AND societe != 'nan'
              AND fonction IS NOT NULL 
              AND fonction != '' 
              AND fonction != 'nan'
              AND telephone IS NOT NULL 
              AND telephone != '' 
              AND telephone != 'nan'
              AND linkedin IS NOT NULL 
              AND linkedin != '' 
              AND linkedin != 'nan'
              AND location IS NOT NULL
              AND location != ''
              AND location != 'nan'
        """))
        
        db.commit()
        
        print(f"✅ {moved_count} leads déplacés vers Gold")
        return {"moved_to_gold": moved_count}

    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")
        


def CompleteSocieteFromEmail(db: Session,base:str):
    try:        
        # 1) Compléter societe depuis la table societe_leads à partir du domaine email
        result1 = db.execute(text(f"""
            UPDATE {base}
            SET societe = s.nom
            FROM societe_leads s
            WHERE ({base}.societe IS NULL OR {base}.societe = '' OR LOWER({base}.societe) = 'nan')
              AND {base}.email IS NOT NULL
              AND {base}.email != ''
              AND LOWER({base}.email) != 'nan'
              AND {base}.email LIKE '%@%.%'
              AND LOWER(TRIM(s.domaine)) = LOWER(TRIM(SPLIT_PART(SPLIT_PART({base}.email, '@', 2), '.', 1)))
              AND (
                    s.extension IS NULL
                    OR s.extension = ''
                    OR LOWER(TRIM(s.extension)) = LOWER(TRIM(SPLIT_PART({base}.email, '.', -1)))
              )
        """))

        # 2) Fallback: déduire la société directement du domaine email (sans table societe_leads)
        # Ex: user@mail.sqli.com -> sqli ; user@partoo.com -> partoo ; user@my-company.fr -> my company
        result2 = db.execute(text(f"""
            UPDATE {base}
            SET societe = INITCAP(
                REPLACE(
                    SPLIT_PART(
                        REGEXP_REPLACE(SPLIT_PART({base}.email, '@', 2), '\\.[^.]+$', ''),
                        '.',
                        -1
                    ),
                    '-',
                    ' '
                )
            )
            WHERE ({base}.societe IS NULL OR {base}.societe = '' OR LOWER({base}.societe) = 'nan')
              AND {base}.email IS NOT NULL
              AND {base}.email != ''
              AND LOWER({base}.email) != 'nan'
              AND {base}.email LIKE '%@%.%'
        """))
        
        db.commit()
        count = (result1.rowcount if hasattr(result1, "rowcount") else 0) + (result2.rowcount if hasattr(result2, "rowcount") else 0)
        
        print(f"✅ {count} sociétés complétées depuis les emails")
        return {"societe_completed": count}

    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")



def CompleteNomPrenomFromEmail(db: Session,base:str):
    try:
        
        # UPDATE avec extraction du nom et prénom en SQL pur (PostgreSQL)
        result = db.execute(text(f"""
            UPDATE {base}
            SET 
                prenom = CASE 
                    WHEN (prenom IS NULL OR prenom = '') 
                         AND POSITION('.' IN SPLIT_PART(email, '@', 1)) > 0
                    THEN INITCAP(SPLIT_PART(SPLIT_PART(email, '@', 1), '.', 1))
                    ELSE prenom
                END,
                nom = CASE 
                    WHEN (nom IS NULL OR nom = '') 
                         AND POSITION('.' IN SPLIT_PART(email, '@', 1)) > 0
                    THEN INITCAP(SPLIT_PART(SPLIT_PART(email, '@', 1), '.', 2))
                    ELSE nom
                END
            WHERE email IS NOT NULL 
              AND email != ''
              AND email LIKE '%@%'
              AND POSITION('.' IN SPLIT_PART(email, '@', 1)) > 0
              AND (
                  (prenom IS NULL OR prenom = '') 
                  OR (nom IS NULL OR nom = '')
              )
        """))
        
        db.commit()
        count = result.rowcount
        
        print(f"✅ {count} noms/prénoms complétés depuis les emails")
        return {"nom_prenom_completed": count}

    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")


def SilverToGold(db:Session,id:int):
    try:
            print("id")
            silver=db.query(Silver_leads).filter(
                Silver_leads.nom != '',
                Silver_leads.nom != 'nan',
                Silver_leads.nom.isnot(None),
                Silver_leads.prenom != '',
                Silver_leads.prenom != 'nan',
                Silver_leads.prenom.isnot(None),
                Silver_leads.email != '',
                Silver_leads.email != 'nan',
                Silver_leads.email.isnot(None),
                Silver_leads.societe != '',
                Silver_leads.societe != 'nan',
                Silver_leads.societe.isnot(None),
                Silver_leads.id==id,
                Silver_leads.fonction != '' ,
                Silver_leads.fonction!= 'nan',
               Silver_leads.fonction.isnot(None),
               Silver_leads.linkedin != '' ,
                Silver_leads.linkedin!= 'nan',
               Silver_leads.linkedin.isnot(None),
               Silver_leads.telephone != '' ,
                Silver_leads.telephone!= 'nan',
               Silver_leads.telephone.isnot(None),
               Silver_leads.location != '',
               Silver_leads.location != 'nan',
               Silver_leads.location.isnot(None)
            ).first()
            if silver is None:
                    raise HTTPException(
                        status_code=400,
                        detail="Lead incomplet → impossible de passer en GOLD"
                    )

            print(silver)

            gold = Gold_leads(
                    email=silver.email,
                    nom=silver.nom,
                    prenom=silver.prenom,
                    fonction=silver.fonction,
                    societe=silver.societe,
                    telephone=silver.telephone,
                    linkedin=silver.linkedin,
                    location=silver.location
                )



            db.add(gold)
            db.delete(silver)
            db.commit()
            return {
                    "message": "Lead ajouté avec succès dans GOLD"
}

    
    except SQLAlchemyError as e:
        db.rollback()
        print(str(e))
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")
regions_villes = {
    "Ile-de-France": {
        "villes": ["Paris", "Versailles", "Boulogne-Billancourt", "Saint-Denis", "Nanterre", "Creteil"]
    },
    "Auvergne-Rhone-Alpes": {
        "villes": ["Lyon", "Grenoble", "Clermont-Ferrand", "Saint-Etienne", "Villeurbanne", "Annecy"]
    },
    "Provence-Alpes-Cote d'Azur": {
        "villes": ["Marseille", "Nice", "Toulon", "Aix-en-Provence", "Avignon", "Cannes"]
    },
    "Occitanie": {
        "villes": ["Toulouse", "Montpellier", "Nimes", "Perpignan", "Beziers", "Albi"]
    },
    "Nouvelle-Aquitaine": {
        "villes": ["Bordeaux", "Limoges", "Poitiers", "La Rochelle", "Bayonne", "Pau"]
    },
    "Hauts-de-France": {
        "villes": ["Lille", "Amiens", "Roubaix", "Tourcoing", "Dunkerque", "Arras"]
    },
    "Grand Est": {
        "villes": ["Strasbourg", "Reims", "Metz", "Nancy", "Mulhouse", "Troyes"]
    },
    "Pays de la Loire": {
        "villes": ["Nantes", "Angers", "Le Mans", "Saint-Nazaire", "Cholet", "La Roche-sur-Yon"]
    },
    "Bretagne": {
        "villes": ["Rennes", "Brest", "Quimper", "Vannes", "Saint-Malo", "Lorient"]
    },
    "Normandie": {
        "villes": ["Rouen", "Caen", "Le Havre", "Cherbourg", "Evreux", "Alencon"]
    },
    "Bourgogne-Franche-Comte": {
        "villes": ["Dijon", "Besancon", "Chalon-sur-Saone", "Nevers", "Montbeliard", "Macon"]
    },
    "Centre-Val de Loire": {
        "villes": ["Tours", "Orleans", "Chartres", "Blois", "Chateauroux", "Bourges"]
    },
    "Corse": {
        "villes": ["Ajaccio", "Bastia", "Corte", "Calvi", "Porto-Vecchio"]
    }
}
def Rephrase(db:Session,base:str):
   result=db.query(Silver_leads).all()
   villeTrouv=""
   regionTrouve=""
   for i in result:
       location = i.location.lower()
       for word in ["greater", "area", "metropolitan"]:
            location = location.replace(word, "")
       for region ,data in regions_villes.items():
        for ville in data["villes"]:
            if ville in location:
                villeTrouv=ville
                regionTrouve=region
                break
        print(villeTrouv,regionTrouve)
                

