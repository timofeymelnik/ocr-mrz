export type AddressHints = {
  streetName: string;
  numero: string;
  piso: string;
  puerta: string;
  cp: string;
  municipio: string;
};

function normalizeSpaces(value: string): string {
  return (value || "").replace(/\s+/g, " ").trim();
}

export function extractAddressHints(rawAddress: string): AddressHints {
  const original = normalizeSpaces(rawAddress);
  if (!original) {
    return { streetName: "", numero: "", piso: "", puerta: "", cp: "", municipio: "" };
  }

  let working = original;
  let numero = "";
  let piso = "";
  let puerta = "";
  let cp = "";
  let municipio = "";

  const cpMatch = working.match(/\b(\d{5})\b/);
  if (cpMatch) {
    cp = cpMatch[1];
    const afterCp = normalizeSpaces(working.slice(cpMatch.index! + cpMatch[0].length));
    if (afterCp) {
      const maybeCity = afterCp.split(",")[0].trim();
      municipio = maybeCity;
    }
  }

  const pisoMatch = working.match(/\b(?:piso|planta)\s*([0-9A-Zºª]{1,4})\b/i);
  if (pisoMatch) piso = pisoMatch[1];
  const puertaMatch = working.match(/\b(?:puerta|pto|door)\s*([0-9A-Z]{1,4})\b/i);
  if (puertaMatch) puerta = puertaMatch[1];

  if (!piso) {
    const compact = working.match(/\b(\d{1,2})\s*([A-Z])\b/);
    if (compact) {
      piso = compact[1];
      puerta = puerta || compact[2];
    }
  }

  const numberMatch = working.match(/\b(?:n[uú]m(?:ero)?\.?\s*)?(\d{1,4}[A-Z]?)\b/i);
  if (numberMatch) numero = numberMatch[1];

  working = working
    .replace(/\b\d{5}\b/g, " ")
    .replace(/\b(?:piso|planta)\s*[0-9A-Zºª]{1,4}\b/gi, " ")
    .replace(/\b(?:puerta|pto|door)\s*[0-9A-Z]{1,4}\b/gi, " ")
    .replace(/\b(?:n[uú]m(?:ero)?\.?\s*)?\d{1,4}[A-Z]?\b/gi, " ");

  return {
    streetName: normalizeSpaces(working),
    numero: numero.trim(),
    piso: piso.trim(),
    puerta: puerta.trim(),
    cp: cp.trim(),
    municipio: municipio.trim(),
  };
}
