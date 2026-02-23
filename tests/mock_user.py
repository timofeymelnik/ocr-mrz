from __future__ import annotations

from copy import deepcopy

MOCK_USER = {
    "identificacion": {
        "nif_nie": "X1234567Z",
        "pasaporte": "P1234567",
        "nombre_apellidos": "EXAMPLE TESTER, ALFA",
        "primer_apellido": "EXAMPLE",
        "segundo_apellido": "TESTER",
        "nombre": "ALFA",
    },
    "domicilio": {
        "tipo_via": "Urbanizacion",
        "nombre_via": "Conjunto Demo",
        "numero": "8A",
        "escalera": "B",
        "piso": "2",
        "puerta": "21",
        "telefono": "600000000",
        "municipio": "DemoCity",
        "provincia": "DemoProvince",
        "cp": "12345",
    },
    "autoliquidacion": {
        "tipo": "principal",
        "num_justificante": "",
        "importe_complementaria": None,
    },
    "tramite": {
        "grupo": "TIE",
        "opcion": "demo",
    },
    "declarante": {
        "localidad": "DemoCity",
        "fecha": "21/02/2026",
        "fecha_dia": "21",
        "fecha_mes": "02",
        "fecha_anio": "2026",
    },
    "ingreso": {
        "forma_pago": "efectivo",
        "iban": "ES0000000000000000000000",
    },
    "extra": {
        "email": "mock.user@example.test",
        "fecha_nacimiento": "12/08/1974",
        "fecha_nacimiento_dia": "12",
        "fecha_nacimiento_mes": "08",
        "fecha_nacimiento_anio": "1974",
        "nacionalidad": "UTO",
        "pais_nacimiento": "Demoland",
        "sexo": "M",
        "estado_civil": "S",
        "lugar_nacimiento": "Demo Region",
        "nombre_padre": "ParentOne",
        "nombre_madre": "ParentTwo",
        "representante_legal": "Rep Demo",
        "representante_documento": "D1234567X",
        "titulo_representante": "Manager",
        "hijos_escolarizacion_espana": "NO",
    },
    "captcha": {"manual": True},
    "download": {"dir": "./downloads", "filename_prefix": "tasa790_012"},
}


def mock_payload() -> dict:
    return deepcopy(MOCK_USER)
