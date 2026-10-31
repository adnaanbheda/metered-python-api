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


async def request_id_header(
    x_request_id: Annotated[str | None, Header()] = None,
) -> None:
    """Expose optional X-Request-Id as a per-request field in Swagger."""
