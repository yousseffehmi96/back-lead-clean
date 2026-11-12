from sqlalchemy import Column, Integer, String, Text, TIMESTAMP, text
from database.db import Base


class ImportJob(Base):
    """Suivi d'un import lancé en tâche de fond.

    L'état vit en base (et non dans un dictionnaire en mémoire comme
    VERIFY_JOBS) pour deux raisons : la progression survit à un redémarrage du
    serveur, et elle reste lisible si plusieurs workers uvicorn tournent — un
    job démarré par le worker A doit pouvoir être interrogé via le worker B.
    """

    __tablename__ = "import_jobs"

    job_id = Column(String, primary_key=True)
    filename = Column(String)
    iduser = Column(Text, index=True)

    # running | done | error
    status = Column(String, default="running")

    # Étape courante (index dans STEPS) et son libellé, affichés tels quels au front
    step = Column(Integer, default=0)
    label = Column(String)
    percent = Column(Integer, default=0)

    # Lignes traitées / attendues (pour un compteur en plus du pourcentage)
    done = Column(Integer, default=0)
    total = Column(Integer, default=0)

    # Résultat final sérialisé en JSON (même charge que l'ancienne réponse
    # synchrone : stats d'upload + résultat du dispatch)
    result = Column(Text)
    error = Column(Text)

    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    finished_at = Column(TIMESTAMP)
