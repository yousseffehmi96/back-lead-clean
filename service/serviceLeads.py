from sqlalchemy.orm import Session
from model.societe_leads import societeleads
from fastapi import HTTPException
from model.silver_leads import Silver_leads
from model.gold_leads import Gold_leads
from model.blacklistLeads import blacklistLeads
from fastapi.responses import StreamingResponse
from model.cleaning_leads import cleaningleads
from model.statistiqueLeads import StatisticLeads
from model.staging_leads import StagingLeads
from model.gold_leads import Gold_leads
from sqlalchemy import or_,and_
from sqlalchemy import text
from datetime import datetime
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
import csv
import io
from sqlalchemy.exc import SQLAlchemyError
from service import service as se
def GetAllSilver(db:Session):
    return db.query(Silver_leads).all()
def GetAllGold(db:Session):
    return db.query(Gold_leads).all()
def GetAllBlack(db:Session):
    return db.query(blacklistLeads).all()
def GetAllClean(db:Session):
    return db.query(cleaningleads).all()
def GetAllStat(db:Session):
    return db.query(StatisticLeads).all()
def GetAllStaging(db:Session):
    return db.query(StagingLeads).all()
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
            "LinkedIn"
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
                lead.linkedin or ""
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
        headers = ["Nom", "Prénom", "Email", "Fonction", "Société", "Téléphone", "LinkedIn"]
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
                lead.linkedin
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
def ToBlack(id:int,eliminer:str,db:Session):
    result =db.query(Silver_leads).filter(Silver_leads.id==id).first()
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
                    eliminer=eliminer
                )
    print(blocklead)
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
            INSERT INTO silver_leads (nom, prenom, email, fonction, societe, telephone, linkedin)
            SELECT DISTINCT ON (email) 
                nom, prenom, email, fonction, societe, telephone, linkedin
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
        
        # 1️⃣ INSERT INTO cleaning_leads depuis staging (évite les doublons)
        result = db.execute(text("""
            INSERT INTO cleaning_leads (nom, prenom, email, fonction, societe, telephone, linkedin)
            SELECT DISTINCT ON (COALESCE(email, id::text)) 
                nom, prenom, email, fonction, societe, telephone, linkedin
            FROM staging_leads sl
            WHERE email IS NULL 
               OR NOT EXISTS (
                   SELECT 1 FROM cleaning_leads cl WHERE cl.email = sl.email
               )
            ORDER BY COALESCE(email, id::text), id
        """))
        
        moved_count = result.rowcount
        
        # 2️⃣ DELETE tous les leads de staging
        db.execute(text("""
            DELETE FROM staging_leads
        """))
        se.SupprimerDoublons(db,"cleaning_leads")
        db.commit()

        
        print(f"✅ {moved_count} leads déplacés vers Cleaning")
        return {"moved_to_clean": moved_count}

    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")

def StagingToGold(db: Session,base:str):
    try:
        
        # 1️⃣ INSERT INTO gold_leads depuis staging (évite les doublons)
        result = db.execute(text(f"""
            INSERT INTO gold_leads (nom, prenom, email, fonction, societe, telephone, linkedin)
            SELECT DISTINCT ON (email) 
                nom, prenom, email, fonction, societe, telephone, linkedin
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
              AND NOT EXISTS (
                  SELECT 1 FROM gold_leads g WHERE g.email = {base}.email
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
              AND fonction IS NOT NULL 
              AND fonction != '' 
              AND fonction != 'nan'
              AND telephone IS NOT NULL 
              AND telephone != '' 
              AND telephone != 'nan'
              AND linkedin IS NOT NULL 
              AND linkedin != '' 
              AND linkedin != 'nan'
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
        

def completevoid(db:Session,base):
    print("hhhhhh")
def CompleteSocieteFromEmail(db: Session,base:str):
    try:        
        # UPDATE avec extraction du domaine en SQL pur
        result = db.execute(text(f"""
            UPDATE {base}
            SET societe = INITCAP(
                REPLACE(
                    SPLIT_PART(SPLIT_PART(email, '@', 2), '.', 1),
                    '-',
                    ' '
                )
            )
            WHERE email IS NOT NULL 
              AND email != ''
              AND email LIKE '%@%.%'
              AND (societe IS NULL OR societe = '')
        """))
        
        db.commit()
        count = result.rowcount
        
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



