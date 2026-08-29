from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictStr, field_validator

from api.schemas.common import ResponseModel


class OpenAIProxyRequest(BaseModel):
    """The stable routing fields plus passthrough OpenAI request options."""

    model_config = ConfigDict(extra="allow")

    model: StrictStr = Field(min_length=1, max_length=255)
    stream: StrictBool = False

    @field_validator("model")
    @classmethod
    def validate_model(cls, value: str) -> str:
        model = value.strip()
        if not model or any(ord(character) < 32 for character in model):
            raise ValueError("model must not be blank or contain control characters")
        return model

    def upstream_payload(self, *, upstream_model: str) -> dict[str, Any]:
        payload = self.model_dump(mode="json")
        payload["model"] = upstream_model
        return payload


class OpenAIModelObject(ResponseModel):
    id: str
    object: Literal["model"] = "model"
    created: int = Field(ge=0)
    owned_by: str


class OpenAIModelList(ResponseModel):
    object: Literal["list"] = "list"
    data: list[OpenAIModelObject]
