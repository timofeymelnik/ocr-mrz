# Tasa 790-012 Auto Filler (Playwright, Python)

Скрипт автоматически заполняет форму:

`https://sede.policia.gob.es/Tasa790_012/ImpresoRellenar`

По умолчанию работает в режиме **manual handoff**:
- заполняет поля заявителя;
- оставляет выбор `Trámite`, CAPTCHA и скачивание человеку.

## Структура

- `main.py` — CLI и запуск сценария
- `form_filler.py` — логика заполнения, выбор Trámite, скачивание
- `validators.py` — загрузка и валидация входного JSON
- `ocr_main.py` — OCR + pipeline orchestration
- `pipeline_runner.py` — стадии pipeline, артефакты, handoff-контракт для UI
- `crm_mapper.py` — маппинг в CRM-профиль клиента

## Требования

- Python 3.11+
- `playwright`

## Установка

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

### CRM (MongoDB)

- Если задан `MONGODB_URI`, CRM хранится в MongoDB (`MONGODB_DB`, `MONGODB_COLLECTION`).
- Если `MONGODB_URI` пустой, включается fallback-хранилище в `runtime/crm_store` (локальные JSON).

## Web UI (Next.js + shadcn)

В репозитории добавлен UI в папке `/Users/tim/WebstormProjects/ocr-mrz/ui`:
- drag&drop upload (pdf/image)
- экран проверки (форма слева + превью исходника справа)
- управляемая browser-session (пользователь сам доходит до нужного шага)
- обучаемый маппер полей: анализ текущей страницы, ручное назначение `canonical_key`, сохранение шаблона
- manual handoff: `Trámite`, CAPTCHA, скачивание выполняет человек

### Обучаемый маппер форм

- Шаблоны маппинга хранятся по `host + path + signature`.
- Версионированные артефакты шаблонов сохраняются в `runtime/artifacts/templates/<target>/<revision>/`:
  - `template.pdf` (если импортировался PDF-шаблон),
  - `mapping.json`,
  - `meta.json`.
- При анализе страницы UI показывает поля (`selector`, `label`, `name/id`) и предложенный `canonical_key`.
- Пользователь корректирует соответствия и нажимает “Сохранить шаблон маппинга”.
- При следующем autofill шаблон применяется автоматически, затем поверх него можно передать ручные overrides.
- Хранилище:
  - MongoDB: коллекция `form_mappings` (или `MONGODB_MAPPING_COLLECTION`)
  - fallback: `runtime/form_mappings/*.json`

### Артефакты и скриншоты

- По умолчанию скриншоты артефактов выключены: `SAVE_ARTIFACT_SCREENSHOTS=0`.
- Сохранение скриншотов при ошибках: `SAVE_ARTIFACT_SCREENSHOTS_ON_ERROR=1`.

### Запуск API

```bash
source .venv/bin/activate
uvicorn web_api:app --reload --port 8000
```

### Локальный Browser Agent (отдельный сервис на клиенте)

Этот сервис нужен для сценария: backend/API крутится на Raspberry Pi, а Chromium запускается на ноуте пользователя.

```bash
source .venv/bin/activate
uvicorn client_browser_agent:app --host 127.0.0.1 --port 8787
```

Проверка:

```bash
curl http://127.0.0.1:8787/health
```

### Запуск UI

```bash
cd ui
cp .env.local.example .env.local
npm install
npm run dev
```

UI: [http://localhost:3000](http://localhost:3000)

## Локальный запуск (кратко)

1. Backend:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
uvicorn web_api:app --host 0.0.0.0 --port 8000 --reload
```

2. Frontend:

```bash
cd ui
cp .env.local.example .env.local
pnpm install
pnpm dev
```

3. Открыть:
- UI: `http://localhost:3000`
- API: `http://localhost:8000`

## Docker (Raspberry Pi 3)

Добавлены:
- `Dockerfile.api` (FastAPI + Chromium для Playwright)
- `Dockerfile.ui` (Next.js production)
- `docker-compose.pi3.yml`

### Важно для Pi 3

- Конфиг рассчитан на `linux/arm/v7`.
- `MONGODB_URI` можно не задавать: тогда CRM работает на fallback-хранилище (`runtime/crm_store`).
- Для MongoDB на Pi 3 обычно удобнее использовать внешний Mongo-инстанс (или Pi с 64-bit OS).

### Запуск

```bash
cp .env.example .env
# (опционально) заполните MONGODB_URI

# если нужен режим "управляемого окна" Playwright (headful):
xhost +local:

docker compose -f docker-compose.pi3.yml up --build
```

Открыть:
- UI: `http://<IP_малинки>:3000`
- API: `http://<IP_малинки>:8000`

### Примечание по Playwright в контейнере

- Режим `Перейти по адресу (открыть управляемое окно)` требует доступ к X11 (`DISPLAY` и `/tmp/.X11-unix` уже проброшены в compose).
- Если контейнер запущен без GUI/X11, используйте только API/UI и ручное заполнение в обычном браузере.

## Входной JSON (пример)

```json
{
  "identificacion": {
    "nif_nie": "X1234567Z",
    "nombre_apellidos": "APELLIDO1 APELLIDO2, NOMBRE"
  },
  "domicilio": {
    "tipo_via": "CALLE",
    "nombre_via": "Gran Via",
    "numero": "10",
    "escalera": "",
    "piso": "2",
    "puerta": "A",
    "telefono": "600000000",
    "municipio": "DemoCity",
    "provincia": "DemoCity",
    "cp": "03001"
  },
  "autoliquidacion": {
    "tipo": "principal"
  },
  "tramite": {
    "grupo": "TIE",
    "opcion": "primera concesión"
  },
  "declarante": {
    "localidad": "DemoCity",
    "fecha": "21/02/2026"
  },
  "ingreso": {
    "forma_pago": "efectivo",
    "iban": ""
  },
  "captcha": {
    "manual": true
  },
  "download": {
    "dir": "./downloads",
    "filename_prefix": "tasa790_012"
  }
}
```

## Запуск

### Из файла

```bash
python main.py --json ./input_payload.json
```

По умолчанию:
- `Trámite` **не** выбирается автоматически;
- CAPTCHA **не** обходится;
- скачивание выполняет человек в браузере.

Можно передавать и OCR-выход (например `output.jsonl` или JSON-документ с `form_790_012` / `forms.790_012`) — скрипт автоматически сделает маппинг полей.
Если в OCR не хватает обязательных полей, скрипт интерактивно спросит их в терминале. Для `Trámite` сначала предложит выбрать группу, затем опцию.

Для OCR-пайплайна (`python ocr_main.py`) выход в `output.jsonl` теперь включает:
- `forms.790_012.fields.email`
- `forms.mi_t.fields.email` (если детектирована форма MI-T или задан `TASA_CODE=mi_t`)
- `forms.visual_generic` для сканов/изображений/рукописных PDF с мягкой (low-confidence) эвристикой.

### Режим visual OCR (рукописные формы)

Для “другого типа тасы” с плохим OCR можно включить:

```bash
TASA_CODE=visual_generic python ocr_main.py
```

Особенности:
- парсер старается вытащить максимум по лейблам (`NIF/NIE`, `APELLIDOS`, `NOMBRE`, `DOMICILIO`, `CP`, `EMAIL`, `FORMA DE PAGO` и т.д.);
- валидация менее строгая (не блокирует документ, если часть полей не распознана);
- в `validation.warnings` добавляется пометка `Visual OCR mode: low-confidence handwritten extraction.`

### Из JSON-строки

```bash
python main.py --json '{"identificacion":{"nif_nie":"X1234567Z","nombre_apellidos":"A B, C"}, ... }'
```

### С флагами

```bash
python main.py \
  --json ./input_payload.json \
  --slowmo 120 \
  --timeout 30000 \
  --download-dir ./downloads
```

Флаги:

- `--headless` — headless режим (обычно выключен, чтобы вручную ввести CAPTCHA)
- `--slowmo` — задержка действий Playwright в мс
- `--timeout` — таймаут ожиданий в мс
- `--download-dir` — переопределить папку скачивания

## Pipeline / CRM / UI-ready output

`python ocr_main.py` теперь пишет в `output.jsonl`:
- parsed form data (`forms.*`);
- `crm_profile` для внутренней CRM;
- `pipeline` со стадиями (`ocr`, `parse_extract_map`, `crm_mapping`) и артефактами;
- `human_tasks` для UI handoff (`verify_filled_fields`, `submit_or_download_manually`).

Это сделано под следующий шаг: веб-интерфейс загрузки документа и ручного выбора типа тасы/Trámite.

## CAPTCHA режим

Скрипт не обходит CAPTCHA. Он:

1. Заполняет все поля.
2. Останавливается перед скачиванием.
3. Печатает: `Введи CAPTCHA на странице и нажми Enter в терминале, чтобы продолжить.`
4. После Enter нажимает `Descargar impreso rellenado`.

## Логика выбора Trámite

- Ищет таблицу по частичному совпадению заголовка группы (`tramite.grupo`).
- Внутри таблицы ищет строку по частичному совпадению текста опции (`tramite.opcion`).
- Выбирает radio в найденной строке.
- Для опций, где требуется число (например, "cada día"/"certificados o informes"), нужно передать `tramite.cantidad` или `tramite.dias`.

## Выход

- Скачанный файл:  
  `"{filename_prefix}_{nif_nie}_{YYYYMMDD}{ext}"`
- Скриншоты этапов в папке downloads:
  - after_fill
  - before_download
  - after_download
  - и error-скриншоты при сбоях

## Troubleshooting

1. CAPTCHA не проходит:
- Убедитесь, что запускаете без `--headless`.
- Введите CAPTCHA вручную в браузере и только потом нажмите Enter в терминале.

2. Таймауты / не находит элементы:
- Увеличьте `--timeout` (например, `--timeout 45000`).
- Увеличьте `--slowmo` для стабильности.
- Сайт мог изменить разметку: проверьте error-скриншот и обновите локаторы в `form_filler.py`.

3. Ошибка при выборе Trámite:
- Проверьте точность `tramite.grupo` и `tramite.opcion` (частичное совпадение текста).
- Для опций с количеством передайте `tramite.cantidad` или `tramite.dias`.

4. Не скачивается файл:
- Возможно, есть валидационные ошибки на форме.
- Проверьте, что все обязательные поля заполнены.
- Если скачался HTML вместо PDF, скрипт сохранит HTML-дамп в папке downloads.

5. Проблемы с IBAN:
- Для `forma_pago = "adeudo"` поле `iban` обязательно и валидируется до старта.
