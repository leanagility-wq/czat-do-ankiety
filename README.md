# Survey Chatbot

Chatbot do odpowiadania na pytania o ankietę. Odpowiedzi są generowane przez LLM, ale wyłącznie na podstawie danych pobranych z PostgreSQL.

## Jak działa

Backend działa w 2 krokach:

1. LLM tworzy plan pobrania danych z ankiety w JSON.
2. Backend wykonuje tylko dozwolone odczyty z bazy, a potem LLM formułuje odpowiedź wyłącznie z przekazanego kontekstu.

Guardrails:

- brak odpowiedzi spoza ankiety
- brak dowolnego SQL generowanego przez model
- brak odpowiedzi bez danych z bazy
- twardy fallback:
  `Tego nie da się stwierdzić na podstawie tej ankiety.`

## Lokalnie

Skopiuj `.env.example` do `.env` i ustaw:

```env
DATABASE_URL=postgresql+asyncpg://survey:survey@localhost:5432/survey_chat
OPENAI_API_KEY=twoj_klucz_openai
OPENAI_MODEL=gpt-5.3-chat-latest
OPENAI_BASE_URL=https://api.openai.com/v1
OPENAI_TIMEOUT_SECONDS=45
```

Instalacja:

```powershell
& "C:\Users\slowo\AppData\Local\Programs\Python\Python311\python.exe" -m pip install -r requirements.txt
```

Uruchomienie:

```powershell
& "C:\Users\slowo\AppData\Local\Programs\Python\Python311\python.exe" -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Otwórz:

- `http://127.0.0.1:8000`
- `http://127.0.0.1:8000/health`

## Pipeline danych

Przetwarzanie:

```powershell
& "C:\Users\slowo\AppData\Local\Programs\Python\Python311\python.exe" scripts/process_survey.py
```

Import do Postgresa:

```powershell
& "C:\Users\slowo\AppData\Local\Programs\Python\Python311\python.exe" import_to_postgres.py
```

## Deploy na Railway

Repo jest przygotowane pod deploy z GitHub przez `Dockerfile`.

Co jest już gotowe:

- aplikacja nasłuchuje na `PORT` nadawanym przez Railway
- `Dockerfile` jest gotowy do builda
- `.dockerignore` nie wpuszcza `.env` i lokalnych danych do obrazu
- `Procfile` zawiera poprawną komendę startową
- `/health` może służyć do prostego sprawdzenia działania

### Co ustawisz w Railway

W serwisie aplikacji dodaj zmienne:

```env
DATABASE_URL=postgresql+asyncpg://...
OPENAI_API_KEY=...
OPENAI_MODEL=gpt-5.3-chat-latest
OPENAI_BASE_URL=https://api.openai.com/v1
OPENAI_TIMEOUT_SECONDS=45
APP_ENV=production
DEBUG=false
```

### Minimalna kolejność wdrożenia

1. Utwórz projekt z repo GitHub.
2. Dodaj serwis `Postgres` w tym samym projekcie Railway.
3. Skopiuj `DATABASE_URL` z Railway Postgres do zmiennych aplikacji.
4. Dodaj `OPENAI_API_KEY` i pozostałe zmienne.
5. Poczekaj na build i deploy aplikacji.
6. Zaimportuj dane do Railway Postgres.

### Jak zaimportować dane do Railway Postgres

Najprościej lokalnie:

1. Skopiuj `DATABASE_URL` z Railway.
2. Ustaw go tymczasowo w lokalnym `.env`.
3. Uruchom:

```powershell
& "C:\Users\slowo\AppData\Local\Programs\Python\Python311\python.exe" import_to_postgres.py
```

Po imporcie przywróć lokalny `DATABASE_URL`, jeśli dalej chcesz pracować z lokalną bazą.

## Bezpieczeństwo sekretów

- `.env` nie jest commitowany do Git i nie powinien trafiać do repo.
- `OPENAI_API_KEY` ustawiaj tylko w Railway Variables albo lokalnym `.env`.
- nie wpisuj klucza do kodu, README, frontendu ani plików `*.js`
- jeśli klucz był gdziekolwiek ujawniony, zrób jego rotację przed produkcją

Szybka kontrola lokalnie:

```powershell
git ls-files .env
rg "OPENAI_API_KEY|sk-" .
```

Pierwsza komenda nie powinna zwrócić `.env`. Druga nie powinna znaleźć prawdziwego klucza w repo.

## Najważniejsze pliki

- `app/services/chat.py`
- `app/services/openai_client.py`
- `app/repositories/survey.py`
- `scripts/process_survey.py`
- `sql/schema.sql`
- `Dockerfile`
