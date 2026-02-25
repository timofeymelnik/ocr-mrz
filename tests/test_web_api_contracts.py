from __future__ import annotations

from fastapi.routing import APIRoute

from web_api import app


def test_health_endpoint_contract_function() -> None:
    route = next(
        (
            candidate
            for candidate in app.routes
            if isinstance(candidate, APIRoute) and candidate.path == "/api/health"
        ),
        None,
    )

    assert route is not None
    payload = route.endpoint()
    assert payload.model_dump() == {"status": "ok"}


def test_openapi_contains_async_task_contracts() -> None:
    schema = app.openapi()

    upload_async = schema["paths"]["/api/documents/upload-async"]["post"]
    assert upload_async["responses"]["202"]["content"]["application/json"]["schema"][
        "$ref"
    ].endswith("TaskAcceptedResponse")

    task_status = schema["paths"]["/api/tasks/{task_id}"]["get"]
    assert task_status["responses"]["200"]["content"]["application/json"]["schema"][
        "$ref"
    ].endswith("TaskStatusResponse")


def test_openapi_contains_error_contract_for_tasks_not_found() -> None:
    schema = app.openapi()
    task_status = schema["paths"]["/api/tasks/{task_id}"]["get"]

    assert task_status["responses"]["404"]["content"]["application/json"]["schema"][
        "$ref"
    ].endswith("ApiErrorResponse")


def test_openapi_contains_auth_rate_limit_contract() -> None:
    schema = app.openapi()
    login = schema["paths"]["/api/auth/login"]["post"]

    assert login["responses"]["429"]["content"]["application/json"]["schema"][
        "$ref"
    ].endswith("ApiErrorResponse")


def test_openapi_contains_documents_and_crm_contracts() -> None:
    schema = app.openapi()

    crm_get = schema["paths"]["/api/crm/documents/{document_id}"]["get"]
    assert crm_get["responses"]["200"]["content"]["application/json"]["schema"][
        "$ref"
    ].endswith("DocumentPayloadResponse")

    confirm = schema["paths"]["/api/documents/{document_id}/confirm"]["post"]
    assert confirm["responses"]["422"]["content"]["application/json"]["schema"][
        "$ref"
    ].endswith("ApiErrorResponse")

    client_match = schema["paths"]["/api/documents/{document_id}/client-match"]["get"]
    assert client_match["responses"]["200"]["content"]["application/json"]["schema"][
        "$ref"
    ].endswith("ClientMatchResponse")

    address_autofill = schema["paths"][
        "/api/documents/{document_id}/address-autofill"
    ]["post"]
    assert address_autofill["responses"]["200"]["content"]["application/json"][
        "schema"
    ]["$ref"].endswith("AddressAutofillResponse")
