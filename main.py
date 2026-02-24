from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from app.autofill.form_filler import fetch_tramite_catalog, fill_for_manual_handoff
from app.core.validators import ValidationError, collect_validation_errors, load_input_payload, validate_payload

FALLBACK_TRAMITE_CATALOG = [
    {
        "group": "Tarjetas de identidad de extranjeros (TIE) y certificados de registro de residentes comunitarios.",
        "options": [
            "TIE de residencia temporal de familiares de personas con nacionalidad española, Certificado de registro de residente comunitario o TIE de familiar de un ciudadano de la Unión y TIE asociada al Acuerdo de Retirada de ciudadanos británicos y sus familiares (BREXIT).",
            "TIE que documenta la primera concesión de la autorización de residencia temporal, de estancia o para trabajadores transfronterizos.",
            "TIE que documenta la renovación de la autorización de residencia temporal o la prórroga de la estancia o de la autorización para trabajadores transfronterizos.",
            "TIE que documenta la autorización de residencia y trabajo de mujeres víctimas de la violencia de género, violencia sexual y trata de seres humanos.",
            "TIE que documenta la autorización de residencia de larga duración o larga duración-UE.",
            "TIE que documenta la residencia de menores tutelados por entidad pública.",
        ],
    },
    {
        "group": "Documentos de identidad y títulos y documentos de viaje a extranjeros indocumentados y otros documentos.",
        "options": [
            "Asignación de Número de Identidad de Extranjero (NIE) a instancia del interesado.",
            "Certificados o informes emitidos a instancia del interesado.",
            "Autorización de regreso.",
            "Cédula de inscripción.",
        ],
    },
]


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Auto-fill Tasa modelo 790-012 form and download generated document."
    )
    parser.add_argument(
        "--json",
        required=True,
        help="JSON input payload. Either a file path or a raw JSON string.",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run Chromium in headless mode (not recommended for manual CAPTCHA).",
    )
    parser.add_argument(
        "--slowmo",
        type=int,
        default=150,
        help="Slow motion delay in milliseconds between Playwright actions.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=20000,
        help="Timeout in milliseconds for actions and waits.",
    )
    parser.add_argument(
        "--download-dir",
        default="",
        help="Override download directory from input JSON.",
    )
    return parser


def main() -> None:
    load_dotenv()
    setup_logging()
    logger = logging.getLogger("main")
    args = build_parser().parse_args()
    try:
        payload = load_input_payload(args.json)
        payload = complete_missing_interactively(payload, args.timeout, require_tramite=False)
        validate_payload(payload, require_tramite=False)
    except ValidationError as exc:
        raise SystemExit(f"Input validation failed:\n{exc}") from exc

    download_dir = args.download_dir.strip() or None
    handoff = fill_for_manual_handoff(
        payload,
        headless=args.headless,
        slowmo=args.slowmo,
        timeout_ms=args.timeout,
        download_dir=download_dir,
    )

    summary = {
        "status": "handoff",
        "headless": args.headless,
        "manual_steps_required": ["verify_filled_fields", "submit_or_download_manually"],
        "screenshot_after_autofill": handoff.get("screenshot", ""),
        "dom_snapshot": handoff.get("dom_snapshot", ""),
    }
    logger.info("Autofill completed. Manual handoff required.")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def complete_missing_interactively(payload: dict, timeout_ms: int, *, require_tramite: bool) -> dict:
    default_phone = os.getenv("DEFAULT_PHONE", "").strip()
    default_forma_pago = os.getenv("DEFAULT_FORMA_PAGO", "efectivo").strip().lower()
    if default_forma_pago not in {"efectivo", "adeudo"}:
        default_forma_pago = "efectivo"

    def ensure(path1: str, path2: str) -> dict:
        payload.setdefault(path1, {})
        payload[path1].setdefault(path2, "")
        return payload[path1]

    def ensure_section(path1: str) -> dict:
        payload.setdefault(path1, {})
        return payload[path1]

    def prompt_required(prompt: str, default: str = "") -> str:
        while True:
            raw = input(prompt).strip()
            if raw:
                return raw
            if default:
                return default
            print("Поле обязательно. Введите значение.")

    for _ in range(3):
        errors = collect_validation_errors(payload, require_tramite=require_tramite)
        if not errors:
            return payload

        print("Обнаружены недостающие поля. Заполните их в терминале.")
        missing = "\n".join(errors)

        if "identificacion.nif_nie" in missing or "identificacion.nif_nie has unexpected format." in missing:
            s = ensure("identificacion", "nif_nie")
            s["nif_nie"] = prompt_required("identificacion.nif_nie: ").upper()
        if "identificacion.nombre_apellidos" in missing:
            s = ensure("identificacion", "nombre_apellidos")
            s["nombre_apellidos"] = prompt_required("identificacion.nombre_apellidos: ")

        if "domicilio.tipo_via" in missing:
            d = ensure("domicilio", "tipo_via")
            d["tipo_via"] = prompt_required("domicilio.tipo_via (например, CALLE): ")
        if "domicilio.nombre_via" in missing:
            d = ensure("domicilio", "nombre_via")
            d["nombre_via"] = prompt_required("domicilio.nombre_via: ")
        if "domicilio.numero" in missing:
            d = ensure("domicilio", "numero")
            d["numero"] = prompt_required("domicilio.numero: ")
        if "domicilio.municipio" in missing:
            d = ensure("domicilio", "municipio")
            d["municipio"] = prompt_required("domicilio.municipio: ")
        if "domicilio.provincia" in missing:
            d = ensure("domicilio", "provincia")
            d["provincia"] = prompt_required("domicilio.provincia: ")
        if "domicilio.cp" in missing or "domicilio.cp must have exactly 5 digits." in missing:
            d = ensure("domicilio", "cp")
            d["cp"] = prompt_required("domicilio.cp (5 цифр): ")

        if "declarante.localidad" in missing:
            d = ensure("declarante", "localidad")
            d["localidad"] = prompt_required("declarante.localidad: ")
        if "declarante.fecha" in missing or "declarante.fecha must be in dd/mm/yyyy format." in missing:
            d = ensure("declarante", "fecha")
            default = datetime.now().strftime("%d/%m/%Y")
            d["fecha"] = prompt_required(f"declarante.fecha (dd/mm/yyyy) [{default}]: ", default=default)

        if "ingreso.forma_pago" in missing or "ingreso.forma_pago must be 'efectivo' or 'adeudo'." in missing:
            i = ensure("ingreso", "forma_pago")
            while True:
                default_choice = "2" if default_forma_pago == "adeudo" else "1"
                choice = (
                    input(f"ingreso.forma_pago [1=efectivo, 2=adeudo] [{default_choice}]: ").strip() or default_choice
                )
                if choice in {"1", "2"}:
                    i["forma_pago"] = "adeudo" if choice == "2" else "efectivo"
                    break
                print("Введите 1 или 2.")
        if str(payload.get("ingreso", {}).get("forma_pago", "")).lower() == "adeudo" and not payload.get("ingreso", {}).get("iban"):
            payload.setdefault("ingreso", {})
            payload["ingreso"]["iban"] = prompt_required("ingreso.iban: ")

        domicilio_section = ensure_section("domicilio")
        if not str(domicilio_section.get("telefono", "")).strip():
            domicilio_section["telefono"] = (
                input(f"domicilio.telefono (опционально) [{default_phone}]: ").strip() or default_phone
            )

        need_group = require_tramite and ("tramite.grupo" in missing or not payload.get("tramite", {}).get("grupo"))
        need_option = require_tramite and ("tramite.opcion" in missing or not payload.get("tramite", {}).get("opcion"))
        if need_group or need_option:
            payload.setdefault("tramite", {})
            catalog = []
            try:
                catalog = fetch_tramite_catalog(timeout_ms=timeout_ms)
            except Exception:
                logging.getLogger("main").exception("Could not fetch trámite catalog from site.")
            if not catalog:
                print("Не удалось получить группы Trámite с сайта, использую встроенный список.")
                catalog = FALLBACK_TRAMITE_CATALOG

            if catalog:
                print("\nДоступные группы Trámite:")
                for idx, g in enumerate(catalog, start=1):
                    print(f"{idx}. {g['group']}")
                while True:
                    raw_group = input("Выберите номер группы [по умолчанию 1]: ").strip() or "1"
                    if raw_group.isdigit() and 1 <= int(raw_group) <= len(catalog):
                        g_idx = int(raw_group)
                        break
                    print(f"Введите число от 1 до {len(catalog)}.")
                group = catalog[g_idx - 1]
                payload["tramite"]["grupo"] = group["group"]

                print("\nОпции выбранной группы:")
                for idx, opt in enumerate(group["options"], start=1):
                    print(f"{idx}. {opt}")
                while True:
                    raw_opt = input("Выберите номер опции [по умолчанию 1]: ").strip() or "1"
                    if raw_opt.isdigit() and 1 <= int(raw_opt) <= len(group["options"]):
                        o_idx = int(raw_opt)
                        break
                    print(f"Введите число от 1 до {len(group['options'])}.")
                payload["tramite"]["opcion"] = group["options"][o_idx - 1]
            else:
                if need_group:
                    payload["tramite"]["grupo"] = prompt_required("tramite.grupo: ")
                if need_option:
                    payload["tramite"]["opcion"] = prompt_required("tramite.opcion: ")

    return payload


if __name__ == "__main__":
    main()
