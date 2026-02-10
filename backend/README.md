# Finn Parser

## Запуск

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
uvicorn app.main:app --reload --port 8000
```

Откройте `http://localhost:8000`.

## Как работает
- Загружает категории и подкатегории с `https://www.finn.no/`.
- Парсит объявления и сохраняет XLSX (`data` + `meta`).
- Ре-чек принимает старый XLSX и выдает `changes` если есть отличия.
