# Tasa 790-012 Auto Filler (Playwright, Python)

Скрипт автоматически заполняет форму:

`https://sede.policia.gob.es/Tasa790_012/ImpresoRellenar`

По умолчанию работает в режиме **manual handoff**:
- заполняет поля заявителя;
- оставляет выбор `Trámite`, CAPTCHA и скачивание человеку.

## Структура

- `web_api.py` — API backend (FastAPI)
- `ui/` — веб-интерфейс (Next.js)
- `app/autofill/*` — логика autofill/manual handoff
- `app/pipeline/*` — стадии pipeline и артефакты
- `app/crm/*` — CRM-профиль и операции с документами

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

- По умолчанию сохраняются только необходимые рабочие артефакты (например, заполненный PDF).
- Скриншоты и HTML-дампы отключены в обычном режиме.
- Для локальной отладки шаблонов включите `TEMPLATE_DEBUG_CAPTURE=1`:
  - подробные снапшоты страницы и инпутов пишутся в `runtime/template_debug/`
  - только в этом режиме учитываются `SAVE_ARTIFACT_SCREENSHOTS` и `SAVE_ARTIFACT_SCREENSHOTS_ON_ERROR`.

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

## API-only режим

CLI-скрипты удалены. Поддерживаются только API и UI флоу.

По умолчанию:
- `Trámite` **не** выбирается автоматически;
- CAPTCHA **не** обходится;
- скачивание выполняет человек в браузере (manual handoff через UI/browser-session).

Pipeline/CRM данные формируются и сохраняются API-процессом:
- parsed form data (`forms.*`);
- `crm_profile`;
- `pipeline` со стадиями (`ocr`, `parse_extract_map`, `crm_mapping`);
- `human_tasks` для handoff (`verify_filled_fields`, `submit_or_download_manually`).

## CAPTCHA режим

Сервис не обходит CAPTCHA. Он:

1. Заполняет все поля.
2. Останавливается перед скачиванием.
3. Передает управление пользователю в browser-session.
4. Пользователь вручную завершает CAPTCHA и скачивание.

## Логика выбора Trámite

- Ищет таблицу по частичному совпадению заголовка группы (`tramite.grupo`).
- Внутри таблицы ищет строку по частичному совпадению текста опции (`tramite.opcion`).
- Выбирает radio в найденной строке.
- Для опций, где требуется число (например, "cada día"/"certificados o informes"), нужно передать `tramite.cantidad` или `tramite.dias`.

## Выход

- Скачанный файл:  
  `"{filename_prefix}_{nif_nie}_{YYYYMMDD}{ext}"`
- Диагностические скриншоты/дампы создаются только при `TEMPLATE_DEBUG_CAPTURE=1`.

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
