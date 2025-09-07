@echo off
setlocal enabledelayedexpansion

REM ===== ScrapMaster - push 1-clic =====
REM A placer a la racine: C:\Users\willi\ScrapMaster\maj_git.cmd

REM 0) URL GitHub par defaut (evite la saisie)
set "ORIGIN_DEFAULT=https://github.com/will383842/ScrapMaster.git"

REM 1) Aller a la racine du projet (dossier du script)
cd /d "%~dp0"

REM 1bis) Verifier que l'on est bien dans le bon dossier (presence de app.py)
if not exist "app.py" (
  echo [ERREUR] Ce script doit etre lance depuis le dossier du projet (app.py introuvable).
  echo Place-le dans C:\Users\willi\ScrapMaster\ et double-clique-le depuis la.
  pause
  exit /b 1
)

REM 2) Verifier Git
where git >nul 2>&1
if errorlevel 1 (
  echo [ERREUR] Git n'est pas installe ou pas dans le PATH.
  echo Installe Git: https://git-scm.com/downloads
  pause
  exit /b 1
)

REM 3) Init depot si besoin
if not exist .git (
  echo Depot Git absent -> initialisation...
  git init || (echo [ERREUR] git init a echoue & pause & exit /b 1)
)

REM 4) Verifier/Configurer le remote "origin"
set "REMOTEURL="
for /f "usebackq tokens=2" %%U in (`git remote -v ^| findstr /b "origin" ^| findstr "(fetch)"`) do set REMOTEURL=%%U

if "%REMOTEURL%"=="" (
  echo Aucun remote "origin" -> ajout de l'URL par defaut
  git remote add origin "%ORIGIN_DEFAULT%" || (echo [ERREUR] ajout remote a echoue & pause & exit /b 1)
  set "REMOTEURL=%ORIGIN_DEFAULT%"
) else (
  echo Remote actuel: %REMOTEURL%
)

REM Corriger si placeholder encore present
echo %REMOTEURL% | findstr /r "<ton-user>/<ton-repo>" >nul
if not errorlevel 1 (
  echo Remote semble etre un placeholder. On va le corriger.
  git remote set-url origin "%ORIGIN_DEFAULT%"
  set "REMOTEURL=%ORIGIN_DEFAULT%"
)

REM 5) Nom de branche -> main
for /f "usebackq" %%B in (`git rev-parse --abbrev-ref HEAD`) do set CURBR=%%B
git branch -M main >nul 2>&1

REM 6) Message de commit (parametre ou prompt)
set "DEFAULT_MSG=chore: maj auto"
if "%~1"=="" (
  set /p MSG=Message de commit (Entrer pour defaut) : 
  if "%MSG%"=="" set "MSG=%DEFAULT_MSG%"
) else (
  set "MSG=%*"
)

REM 7) Staging + commit si changements
set CHANGED=0
for /f %%A in ('git status --porcelain') do set CHANGED=1

git add -A

if %CHANGED%==1 (
  git commit -m "%MSG%"
  if errorlevel 1 (
    echo [ERREUR] Le commit a echoue.
    pause & exit /b 1
  )
) else (
  echo Rien a committer. On pousse quand meme...
)

REM 7bis) (optionnel) recuperer d'abord le remote pour eviter les non-fast-forward
git pull --rebase origin main >nul 2>&1

REM 8) Push
git push -u origin main
if errorlevel 1 (
  echo.
  echo [ERREUR] Le push a echoue.
  echo - Verifie l'URL: %REMOTEURL%
  echo - Si identifiants demandes: Username = ton login GitHub, Password = ton PAT (token)
  echo   Astuce pour memoriser: git config --global credential.helper manager
  pause
  exit /b 1
)

echo.
echo OK : depot a jour sur %REMOTEURL%
pause
exit /b 0
