export type Payload = {
  identificacion: {
    nif_nie: string;
    pasaporte?: string;
    documento_tipo?: "pasaporte" | "nif_tie_nie_dni";
    nombre_apellidos: string;
    primer_apellido?: string;
    segundo_apellido?: string;
    nombre?: string;
  };
  domicilio: {
    tipo_via: string;
    nombre_via: string;
    numero: string;
    escalera: string;
    piso: string;
    puerta: string;
    telefono: string;
    municipio: string;
    provincia: string;
    cp: string;
  };
  autoliquidacion: {
    tipo: string;
    num_justificante?: string;
    importe_complementaria?: number | null;
  };
  tramite: {
    grupo?: string;
    opcion?: string;
    cantidad?: string;
    dias?: string;
  };
  declarante: {
    localidad: string;
    fecha: string;
    fecha_dia?: string;
    fecha_mes?: string;
    fecha_anio?: string;
  };
  ingreso: {
    forma_pago: string;
    iban: string;
  };
  extra?: {
    email?: string;
    telefono_country_iso?: string;
    fecha_nacimiento?: string;
    fecha_nacimiento_dia?: string;
    fecha_nacimiento_mes?: string;
    fecha_nacimiento_anio?: string;
    nacionalidad?: string;
    pais_nacimiento?: string;
    sexo?: string;
    estado_civil?: string;
    lugar_nacimiento?: string;
    nombre_padre?: string;
    nombre_madre?: string;
    representante_legal?: string;
    representante_documento?: string;
    titulo_representante?: string;
    hijos_escolarizacion_espana?: string;
  };
  captcha: {
    manual: boolean;
  };
  download: {
    dir: string;
    filename_prefix: string;
  };
};

export type MergeCandidate = {
  document_id: string;
  name: string;
  document_number: string;
  updated_at: string;
  score: number;
  reasons: string[];
  identity_overlap: string[];
  name_overlap: string[];
};

export type AddressAutofillResponse = {
  document_id: string;
  address_line: string;
  normalized_address: string;
  geocode_used: boolean;
  domicilio: {
    tipo_via: string;
    nombre_via: string;
    numero: string;
    escalera: string;
    piso: string;
    puerta: string;
    municipio: string;
    provincia: string;
    cp: string;
  };
};

export type UploadResponse = {
  document_id: string;
  client_id?: string;
  source?: Record<string, unknown>;
  preview_url: string;
  form_url: string;
  target_url?: string;
  source_kind_input?: string;
  source_kind_detected?: string;
  source_kind_confidence?: number;
  source_kind_auto?: boolean;
  source_kind_requires_review?: boolean;
  payload: Payload;
  document: Record<string, unknown>;
  missing_fields: string[];
  validation_issues?: string[];
  manual_steps_required: string[];
  identity_match_found?: boolean;
  identity_source_document_id?: string;
  workflow_stage?: string;
  workflow_next_step?: string;
  client_match?: MergeCandidate;
  client_match_decision?: string;
  merge_candidates?: MergeCandidate[];
  enrichment_preview?: Array<{
    field: string;
    current_value: string;
    suggested_value: string;
    source?: string;
  }>;
  enrichment_skipped?: Array<{
    field: string;
    current_value: string;
    suggested_value: string;
    source?: string;
    reason?: string;
  }>;
};

export type ClientMatchResponse = {
  document_id: string;
  identity_match_found: boolean;
  identity_source_document_id?: string;
  client_match?: MergeCandidate;
  client_match_decision?: string;
  merge_candidates: MergeCandidate[];
  workflow_stage: string;
  workflow_next_step: string;
};

export type AutofillPreviewResponse = {
  document_id: string;
  form_url: string;
  status?: string;
  error_code?:
    | "TEMPLATE_NOT_FOUND"
    | "TEMPLATE_INVALID"
    | "FILL_PARTIAL"
    | "FILL_FAILED";
  message?: string;
  filled_pdf_url?: string;
};

export type EnrichByIdentityResponse = {
  document_id: string;
  identity_match_found: boolean;
  identity_source_document_id?: string;
  identity_key?: string;
  applied_fields: string[];
  skipped_fields: string[];
  merge_candidates: MergeCandidate[];
  enrichment_preview: Array<{
    field: string;
    current_value: string;
    suggested_value: string;
    source?: string;
    reason?: string;
  }>;
  enrichment_skipped?: Array<{
    field: string;
    current_value: string;
    suggested_value: string;
    source?: string;
    reason?: string;
  }>;
  missing_fields: string[];
  validation_issues?: string[];
  payload: Payload;
};

export type AutofillValidationResponse = {
  status: string;
  matches: boolean;
  field_report: Array<{
    selector: string;
    canonical_key: string;
    expected: string | boolean;
    actual: string | boolean;
    ok: boolean;
    reason: string;
  }>;
  missing: string[];
  unexpected: string[];
  summary: {
    total_checked: number;
    matched: number;
    mismatched: number;
    unfilled_required: number;
  };
  filled_pdf_path?: string;
  filled_pdf_url?: string;
  template_updated_at?: string;
  template_source?: string;
};

export type FieldSuggestion = {
  selector: string;
  tag?: string;
  type?: string;
  id?: string;
  name?: string;
  label?: string;
  placeholder?: string;
  aria_label?: string;
  canonical_key?: string;
  field_kind?: "text" | "select" | "checkbox" | "radio";
  match_value?: string;
  checked_when?: string;
  confidence?: number;
  source?: string;
  value_preview?: string;
};

export type AnalyzeFieldsResponse = {
  document_id: string;
  session_id: string;
  current_url: string;
  fields: FieldSuggestion[];
  suggestions: FieldSuggestion[];
  template_mappings?: Array<{
    selector: string;
    canonical_key?: string;
    field_kind?: "text" | "select" | "checkbox" | "radio";
    match_value?: string;
    checked_when?: string;
    source?: string;
    confidence?: number;
  }>;
  canonical_keys: string[];
  template_loaded: boolean;
};

export type SavedCrmDocument = {
  document_id: string;
  client_id?: string;
  document_number: string;
  name: string;
  updated_at: string;
  status: string;
  has_edited: boolean;
};
