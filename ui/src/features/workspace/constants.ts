export type Step =
  | "upload"
  | "match"
  | "merge"
  | "review"
  | "prepare"
  | "autofill";

export type UploadSourceKind =
  | ""
  | "anketa"
  | "fmiliar"
  | "passport"
  | "nie_tie"
  | "visa";

export const API_BASE: string =
  process.env.NEXT_PUBLIC_API_BASE || "http://localhost:8000";

export const CLIENT_AGENT_BASE: string =
  process.env.NEXT_PUBLIC_CLIENT_AGENT_BASE || "http://127.0.0.1:8787";

export const TARGET_URL_PRESETS = [
  {
    key: "doc17_tie",
    label: "Doc 17 - Formulario TIE",
    url: "https://www.inclusion.gob.es/documents/410169/2156469/17-Formulario_TIE.pdf",
  },
  {
    key: "doc13_autoriz_regreso",
    label: "Doc 13 - Autorización de regreso",
    url: "https://www.inclusion.gob.es/documents/d/migraciones/13-formulario_autoriz_de_regreso",
  },
  {
    key: "doc11_larga_duracion",
    label: "Doc 11 - Larga duración",
    url: "https://www.inclusion.gob.es/documents/410169/2156469/11-Formulario_larga_duracixn.pdf",
  },
  {
    key: "tasa_790_052",
    label: "Tasa 790-052",
    url: "https://sede.administracionespublicas.gob.es/tasasPDF/prepareProvincia?idModelo=790&idTasa=052",
  },
  {
    key: "tasa_790_012",
    label: "Tasa 790-012",
    url: "https://sede.policia.gob.es/Tasa790_012/",
  },
] as const;
