export type DateParts = { day: string; month: string; year: string };

export type FullNameParts = {
  primer_apellido: string;
  segundo_apellido: string;
  nombre: string;
};

export type NieParts = { prefix: string; number: string; suffix: string };

export const toUrl = (path: string, base: string): string => {
  if (!path) {
    return "";
  }
  if (path.startsWith("http://") || path.startsWith("https://")) {
    return path;
  }
  return `${base}${path}`;
};

export const isPdfTargetUrl = (value: string): boolean => {
  const normalizedValue = value.trim().toLowerCase();
  if (!normalizedValue) {
    return false;
  }
  return (
    /\.pdf(?:$|\?)/i.test(normalizedValue) ||
    normalizedValue.includes("/documents/d/") ||
    normalizedValue.includes("/documents/")
  );
};

export const ddmmyyyyToIso = (value: string): string => {
  const normalizedValue = value.trim();
  const match = normalizedValue.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (!match) {
    return "";
  }
  return `${match[3]}-${match[2]}-${match[1]}`;
};

export const isoToDdmmyyyy = (value: string): string => {
  const normalizedValue = value.trim();
  const match = normalizedValue.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) {
    return "";
  }
  return `${match[3]}/${match[2]}/${match[1]}`;
};

export const splitDdmmyyyy = (value: string): DateParts => {
  const normalizedValue = value.trim();
  const match = normalizedValue.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (!match) {
    return { day: "", month: "", year: "" };
  }
  return { day: match[1], month: match[2], year: match[3] };
};

export const composeDdmmyyyy = (
  day: string,
  month: string,
  year: string,
): string => {
  const normalizedDay = day.replace(/\D/g, "").slice(0, 2);
  const normalizedMonth = month.replace(/\D/g, "").slice(0, 2);
  const normalizedYear = year.replace(/\D/g, "").slice(0, 4);

  if (
    normalizedDay.length === 2 &&
    normalizedMonth.length === 2 &&
    normalizedYear.length === 4
  ) {
    return `${normalizedDay}/${normalizedMonth}/${normalizedYear}`;
  }
  return "";
};

export const splitFullName = (value: string): FullNameParts => {
  const normalizedValue = value.trim();
  if (!normalizedValue) {
    return { primer_apellido: "", segundo_apellido: "", nombre: "" };
  }

  if (normalizedValue.includes(",")) {
    const [left, right] = normalizedValue.split(",", 2).map((token) => token.trim());
    const parts = left.split(/\s+/).filter(Boolean);
    return {
      primer_apellido: parts[0] || "",
      segundo_apellido: parts.slice(1).join(" "),
      nombre: right || "",
    };
  }

  const parts = normalizedValue.split(/\s+/).filter(Boolean);
  if (parts.length === 1) {
    return { primer_apellido: parts[0], segundo_apellido: "", nombre: "" };
  }
  if (parts.length === 2) {
    return { primer_apellido: parts[0], segundo_apellido: "", nombre: parts[1] };
  }
  return {
    primer_apellido: parts[0],
    segundo_apellido: parts[1],
    nombre: parts.slice(2).join(" "),
  };
};

export const composeFullName = (
  primerApellido: string,
  segundoApellido: string,
  nombre: string,
): string => {
  const left = [primerApellido.trim(), segundoApellido.trim()]
    .filter(Boolean)
    .join(" ")
    .trim();
  const right = nombre.trim();
  if (left && right) {
    return `${left}, ${right}`;
  }
  return left || right;
};

export const parseNieParts = (value: string): NieParts => {
  const sanitized = value.toUpperCase().replace(/[^A-Z0-9]/g, "");
  const match = sanitized.match(/^([XYZ])(\d{7})([A-Z])$/);
  if (!match) {
    return { prefix: "", number: "", suffix: "" };
  }
  return { prefix: match[1], number: match[2], suffix: match[3] };
};

export const composeNie = (
  prefix: string,
  number: string,
  suffix: string,
): string => {
  const normalizedPrefix = prefix.toUpperCase().replace(/[^XYZ]/g, "");
  const normalizedNumber = number.replace(/\D/g, "").slice(0, 7);
  const normalizedSuffix = suffix.toUpperCase().replace(/[^A-Z]/g, "").slice(0, 1);
  if (
    normalizedPrefix &&
    normalizedNumber.length === 7 &&
    normalizedSuffix
  ) {
    return `${normalizedPrefix}${normalizedNumber}${normalizedSuffix}`;
  }
  return "";
};

export const readErrorResponse = async (resp: Response): Promise<string> => {
  const text = await resp.text();
  if (!text) {
    return `Request failed (${resp.status})`;
  }

  try {
    const parsed = JSON.parse(text) as { detail?: string; message?: string };
    if (parsed.detail) {
      return parsed.detail;
    }
    if (parsed.message) {
      return parsed.message;
    }
  } catch {
    return text;
  }

  return text;
};
