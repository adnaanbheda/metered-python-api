from typing import Annotated

from fastapi import Header, Request

from mock_data import ORGS


def examples(values: dict[str, str]) -> dict:
    """Swagger dropdown entries from {value: summary}; fields stay free text."""
    return {
        value: {"summary": summary, "value": value} for value, summary in values.items()
    }


def org_summary(org: str) -> str:
    limits = ", ".join(f"{feature} {limit}" for feature, limit in ORGS[org].items())
    return f"{org}: {limits or 'no limits (403)'}"


ORG_EXAMPLES = examples({org: org_summary(org) for org in ORGS})


async def org_id_header(
    request: Request,
    x_org_id: Annotated[str | None, Header(openapi_examples=ORG_EXAMPLES)] = None,
) -> str:
    """Expose X-Org-Id in Swagger; return the org IdentityMiddleware already validated."""
    return request.state.org_id


def optional_header(name: str, title: str, **extra) -> dict:
    return {
        "name": name,
        "in": "header",
        "required": False,
        "schema": {"anyOf": [{"type": "string"}, {"type": "null"}], "title": title},
        **extra,
    }


# X-Org-Id and optional X-Request-Id in Swagger for metered routes, as OpenAPI only:
# a Depends would run on every request, and the middlewares already read both headers.
OPERATION_HEADERS = {
    "parameters": [
        optional_header("x-org-id", "X-Org-Id", examples=ORG_EXAMPLES),
        optional_header("x-request-id", "X-Request-Id"),
    ]
}
