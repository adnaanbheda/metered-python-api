from fastapi.responses import JSONResponse

from schemas import ErrorResponse


def error_response(
    status: int, code: str, detail: str, headers: dict[str, str]
) -> JSONResponse:
    body = ErrorResponse(detail=detail, code=code).model_dump()
    return JSONResponse(body, status_code=status, headers=headers)
