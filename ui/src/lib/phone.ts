export type PhoneCountryIso = "ES" | "RU" | "FR" | "DE" | "IT" | "PT" | "PL" | "RO";

export type PhoneCountryOption = {
  iso: PhoneCountryIso;
  label: string;
  flag: string;
  dialCode: string;
  minDigits: number;
  maxDigits: number;
};

export const PHONE_COUNTRIES: PhoneCountryOption[] = [
  { iso: "ES", label: "Spain", flag: "🇪🇸", dialCode: "+34", minDigits: 9, maxDigits: 9 },
  { iso: "RU", label: "Russia", flag: "🇷🇺", dialCode: "+7", minDigits: 10, maxDigits: 10 },
  { iso: "FR", label: "France", flag: "🇫🇷", dialCode: "+33", minDigits: 9, maxDigits: 9 },
  { iso: "DE", label: "Germany", flag: "🇩🇪", dialCode: "+49", minDigits: 10, maxDigits: 11 },
  { iso: "IT", label: "Italy", flag: "🇮🇹", dialCode: "+39", minDigits: 9, maxDigits: 10 },
  { iso: "PT", label: "Portugal", flag: "🇵🇹", dialCode: "+351", minDigits: 9, maxDigits: 9 },
  { iso: "PL", label: "Poland", flag: "🇵🇱", dialCode: "+48", minDigits: 9, maxDigits: 9 },
  { iso: "RO", label: "Romania", flag: "🇷🇴", dialCode: "+40", minDigits: 9, maxDigits: 9 },
];

const DEFAULT_PHONE_ISO: PhoneCountryIso = "ES";

function digitsOnly(value: string): string {
  return (value || "").replace(/\D/g, "");
}

function getCountryByIso(iso: string | undefined): PhoneCountryOption {
  return PHONE_COUNTRIES.find((item) => item.iso === iso) || PHONE_COUNTRIES[0];
}

function findCountryByDialCode(code: string): PhoneCountryOption | null {
  const normalized = (code || "").replace(/\D/g, "");
  if (!normalized) return null;
  const sorted = [...PHONE_COUNTRIES].sort((a, b) => b.dialCode.length - a.dialCode.length);
  return (
    sorted.find((item) => normalized.startsWith(item.dialCode.replace(/\D/g, ""))) || null
  );
}

export function parsePhoneParts(value: string): {
  countryIso: PhoneCountryIso;
  countryCode: string;
  localNumber: string;
} {
  const raw = (value || "").trim();
  if (!raw) {
    const country = getCountryByIso(DEFAULT_PHONE_ISO);
    return {
      countryIso: country.iso,
      countryCode: country.dialCode,
      localNumber: "",
    };
  }

  const compact = raw.replace(/\s+/g, "");
  const withPlus = compact.match(/^\+(\d{1,3})(\d*)$/);
  if (withPlus) {
    const country = findCountryByDialCode(withPlus[1]);
    if (country) {
      const dialDigits = country.dialCode.replace(/\D/g, "");
      return {
        countryIso: country.iso,
        countryCode: country.dialCode,
        localNumber: withPlus[1].slice(dialDigits.length) + (withPlus[2] || ""),
      };
    }
  }

  const country = getCountryByIso(DEFAULT_PHONE_ISO);
  return {
    countryIso: country.iso,
    countryCode: country.dialCode,
    localNumber: digitsOnly(compact),
  };
}

export function composePhone(countryIso: PhoneCountryIso, localNumber: string): string {
  const country = getCountryByIso(countryIso);
  const local = digitsOnly(localNumber).slice(0, 15);
  if (!local) return "";
  return `${country.dialCode}${local}`;
}

export function validatePhone(
  countryIso: PhoneCountryIso,
  localNumber: string,
): { valid: boolean; message: string } {
  const country = getCountryByIso(countryIso);
  const local = digitsOnly(localNumber);
  if (!local) return { valid: true, message: "" };
  if (local.length < country.minDigits || local.length > country.maxDigits) {
    const expected =
      country.minDigits === country.maxDigits
        ? `${country.minDigits}`
        : `${country.minDigits}-${country.maxDigits}`;
    return {
      valid: false,
      message: `Для ${country.iso} нужно ${expected} цифр после кода ${country.dialCode}.`,
    };
  }
  return { valid: true, message: "" };
}
