# Survey Processing Pipeline

Mały pipeline w Pythonie 3.11, który bierze surowy plik ankiety `input/raw_survey.csv` i przygotowuje zestaw plików pod chatbot odpowiadający wyłącznie na podstawie danych z ankiety.

## Struktura

```text
input/
  raw_survey.csv
output/
  .gitkeep
scripts/
  process_survey.py
sql/
  schema.sql
import_to_postgres.py
requirements.txt
README.md
```

## Co generuje pipeline

Po uruchomieniu [scripts/process_survey.py](/d:/Programowanie/czat-ankieta/scripts/process_survey.py) powstają:

- `output/responses_clean.csv`
- `output/question_metadata.csv`
- `output/aggregates.csv`
- `output/correlations.csv`
- `output/open_topics.csv`

## Co robi skrypt

- czyści puste kolumny i techniczne śmieci
- dodaje `response_id`, jeśli nie ma go w źródle
- mapuje nazwy kolumn do `snake_case`
- zachowuje oryginalne nazwy pytań w `question_metadata.csv`
- normalizuje role do wspólnych kategorii
- grupuje rzadkie role z pojedynczym wystąpieniem do `Inne`
- wykrywa pytania skalarne, otwarte i wielokrotnego wyboru
- tworzy gotowe agregaty i korelacje Spearmana
- przypisuje 1-3 heurystyczne tematy do odpowiedzi otwartych

## Najważniejsze miejsce do ręcznej korekty

Najłatwiej poprawić mapowanie kolumn w stałej `MANUAL_COLUMN_HINTS` na początku pliku [scripts/process_survey.py](/d:/Programowanie/czat-ankieta/scripts/process_survey.py). To tam definiowane są reguły, które łączą tekst nagłówka z docelowym polem, np. `ai_usage_frequency` albo `role_confidence`.

## Instalacja

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Dry run

Tryb podglądu nie zapisuje plików. Pokazuje:

- wykryte kolumny
- proponowane mapowanie
- listę pytań otwartych
- listę pytań skalowych

Uruchom:

```powershell
python scripts/process_survey.py --dry-run
```

## Pełne przetwarzanie

```powershell
python scripts/process_survey.py
```

Opcjonalnie możesz wskazać własne ścieżki:

```powershell
python scripts/process_survey.py --input input/raw_survey.csv --output-dir output
```

## Import do PostgreSQL

1. Ustaw `DATABASE_URL`.

Przykład:

```powershell
$env:DATABASE_URL="postgresql://survey:survey@localhost:5432/survey_chat"
```

2. Zaimportuj wygenerowane pliki:

```powershell
python import_to_postgres.py
```

Skrypt:

- wczytuje [sql/schema.sql](/d:/Programowanie/czat-ankieta/sql/schema.sql)
- tworzy tabele
- czyści tabele przed importem
- ładuje dane z katalogu `output/`

## Uwagi do heurystyk

- role są rozpoznawane po słowach takich jak `Scrum Master`, `Project Manager`, `Agile Coach`, `Product Owner`, `Product Manager`, `Delivery`
- pytania otwarte są wykrywane po długości i różnorodności odpowiedzi
- wartości multi-choice są rozdzielane po przecinku, średniku, pionowej kresce i nowej linii
- `company_ai_actions` w `aggregates.csv` jest traktowane jako proxy oparte na pytaniu o poziom inwestycji firmy w AI, bo w tym surowym pliku nie ma osobnego pola o działaniach firm

## Pliki wynikowe

`responses_clean.csv`

- znormalizowane odpowiedzi na poziomie pojedynczej odpowiedzi

`question_metadata.csv`

- metadane pól, typ pytania, skale i wartości dopuszczalne

`aggregates.csv`

- gotowe agregaty pod chatbot bez potrzeby liczenia wszystkiego na żywo

`correlations.csv`

- korelacje Spearmana dla kluczowych par metryk, także per rola jeśli próba na to pozwala

`open_topics.csv`

- przypisane tematy i cytaty z odpowiedzi otwartych
