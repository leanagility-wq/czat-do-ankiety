# Survey Chatbot

Lokalny chatbot do pytań o ankietę. Odpowiedzi są generowane przez LLM, ale wyłącznie na podstawie danych pobranych z PostgreSQL z tabel `question_metadata`, `aggregates`, `correlations` i `open_topics`.

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

## Wymagane zmienne środowiskowe

Skopiuj `.env.example` do `.env` i ustaw:

```env
DATABASE_URL=postgresql+asyncpg://survey:survey@localhost:5432/survey_chat
OPENAI_API_KEY=twoj_klucz_openai
OPENAI_MODEL=gpt-5.3-chat-latest
```

## Instalacja

```powershell
& "C:\Users\slowo\AppData\Local\Programs\Python\Python311\python.exe" -m pip install -r requirements.txt
```

## Uruchomienie

```powershell
& "C:\Users\slowo\AppData\Local\Programs\Python\Python311\python.exe" -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Otwórz:

- `http://127.0.0.1:8000`
- `http://127.0.0.1:8000/health`

## Pipeline danych

Surową ankietę przetwarza:

```powershell
& "C:\Users\slowo\AppData\Local\Programs\Python\Python311\python.exe" scripts/process_survey.py
```

Import do Postgresa:

```powershell
& "C:\Users\slowo\AppData\Local\Programs\Python\Python311\python.exe" import_to_postgres.py
```

## Najważniejsze pliki

- `app/services/chat.py`
- `app/services/openai_client.py`
- `app/repositories/survey.py`
- `scripts/process_survey.py`
- `sql/schema.sql`
