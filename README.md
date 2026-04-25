# ProspectHunter (C-Y ASS)

ProspectHunter est une API FastAPI de prospection locale B2B.
Signature produit: `C-Y ASS`.

## Fonctions principales
- Recherche par `query + city + country`
- Source `overpass` (par defaut, gratuite) ou `google_maps` (optionnelle)
- Recuperation des contacts dispo: telephone + email (si trouve sur le site web de l'etablissement)
- Export CSV/XLSX
- Mini CRM multi-utilisateur via `api_key`
- Interface web fullstack deployable via `GET /`

## Structure web
- `main.py` : API FastAPI + routage du dashboard
- `templates/index.html` : interface principale
- `static/app.css` : theme et layout
- `static/app.js` : chargement jobs/leads, filtres, exports et CRM

## API rapide
- `GET /health`
- `GET /search?query=restaurant&city=Paris&country=France&source=overpass&weakness=all&limit=120&api_key=dev-key-change-me`
- `GET /jobs?api_key=dev-key-change-me`
- `GET /jobs/{job_id}?api_key=dev-key-change-me`
- `GET /jobs/{job_id}/leads?limit=100&offset=0&priority=HOT&status=contacted&tag=no_website&api_key=dev-key-change-me`
- `GET /crm/update?lead_id=abc123&status=contacted&note=Premier_message&api_key=dev-key-change-me`
- `GET /crm/summary?api_key=dev-key-change-me`
- `GET /` (dashboard web)

## Variables d'environnement
- `PROSPECT_API_KEY=dev-key-change-me`
- `GOOGLE_MAPS_API_KEY=...` (optionnelle, seulement si tu utilises `source=google_maps`)
- `PORT=8000`

Copie:
```bash
cp .env.example .env
```

## Lancement local
```bash
pip install -r requirements.txt
set PROSPECT_API_KEY=dev-key-change-me
set GOOGLE_MAPS_API_KEY=votre-cle-api-google  # optionnel
uvicorn main:app --reload --host 127.0.0.1 --port 8000
```

Puis ouvre `http://127.0.0.1:8000`.

## Mise en ligne (site web)

### Option 1: Railway
1. Push du repo sur GitHub.
2. New Project + Deploy from GitHub.
3. Railway utilise `railway.json`.
4. Ajoute les variables d'environnement (`PROSPECT_API_KEY` obligatoire, `GOOGLE_MAPS_API_KEY` optionnelle).

Note: la base SQLite est stockee dans `data/`. Sur Railway, ce stockage reste local au conteneur et peut etre perdu lors d'un redeploiement ou restart. Pour un CRM durable en production, prevoir ensuite un stockage persistant ou une base externe.

### Option 2: Docker
```bash
docker compose up --build -d
```

## Application Android (Play Store ready base)
Le dossier `android/` contient une app WebView `ProspectHunter C-Y ASS`.

Avant build:
1. Mets ton URL de prod dans `android/app/src/main/res/values/strings.xml` (`base_url`).
2. Build APK/AAB.

Build local (si Android SDK + Gradle installes):
```bash
cd android
gradle :app:assembleDebug
```

CI GitHub produit aussi un APK debug via `.github/workflows/ci.yml`.

## Publication Play Store (compte developpeur requis)
1. Creer une cle de signature (keystore).
2. Configurer le build `release` signe.
3. Generer AAB: `gradle :app:bundleRelease`.
4. Upload dans Google Play Console.

## GitHub
Init/push rapide:
```bash
git init
git add .
git commit -m "Finalize ProspectHunter C-Y ASS: web + android + deploy"
git branch -M main
git remote add origin https://github.com/<votre-user>/<votre-repo>.git
git push -u origin main
```

## Conformite
- Pas de contournement anti-bot/captcha
- Donnees publiques uniquement
- Respect CGU plateformes et lois locales (RGPD, anti-spam)
