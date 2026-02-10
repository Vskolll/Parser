# Finn Parser Bot

## Запуск

```bash
cd /Users/nikita/Parser/backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export BOT_TOKEN="<PASTE_TOKEN_HERE>"
python -m bot.bot
```

## Команды
- `/start` — выбрать подкатегорию Torget
- `/parse` — парс и выдача XLSX
- `/recheck` — отправь XLSX, бот вернет изменения
