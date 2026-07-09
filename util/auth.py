"""
Vérification d'authentification via JWT Clerk (sans réseau au-delà du JWKS mis en cache).

- Le frontend envoie le token de session Clerk dans `Authorization: Bearer <token>`
  (ou `?__token=<token>` pour les téléchargements via window.open).
- On valide la signature (RS256) avec le JWKS de Clerk, l'issuer et l'expiration.
- Activation contrôlée par la variable d'env REQUIRE_AUTH (déploiement progressif).
"""
import os
import jwt
from jwt import PyJWKClient
from fastapi import Request, HTTPException

CLERK_ISSUER = (os.getenv("CLERK_ISSUER") or "").rstrip("/")
REQUIRE_AUTH = (os.getenv("REQUIRE_AUTH") or "false").strip().lower() in ("1", "true", "yes", "on")

_JWKS_CLIENT = None


def _jwks_client():
    global _JWKS_CLIENT
    if _JWKS_CLIENT is None and CLERK_ISSUER:
        # PyJWKClient met en cache les clés publiques (pas d'appel réseau à chaque requête)
        _JWKS_CLIENT = PyJWKClient(f"{CLERK_ISSUER}/.well-known/jwks.json")
    return _JWKS_CLIENT


def _extract_token(request: Request) -> str:
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth[7:].strip()
    # Fallback pour les téléchargements (window.open ne peut pas poser de header)
    return (request.query_params.get("__token") or "").strip()


def require_auth(request: Request):
    """Dépendance FastAPI : exige un token Clerk valide (si REQUIRE_AUTH est activé)."""
    if not REQUIRE_AUTH:
        return {"disabled": True}

    if not CLERK_ISSUER:
        raise HTTPException(status_code=500, detail="CLERK_ISSUER non configuré côté backend")

    token = _extract_token(request)
    if not token:
        raise HTTPException(status_code=401, detail="Authentification requise")

    try:
        signing_key = _jwks_client().get_signing_key_from_jwt(token).key
        claims = jwt.decode(
            token,
            signing_key,
            algorithms=["RS256"],
            issuer=CLERK_ISSUER,
            options={"verify_aud": False},   # les tokens de session Clerk n'ont pas d'audience fixe
        )
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expiré")
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Token invalide : {e}")

    return {"user_id": claims.get("sub"), "claims": claims}
