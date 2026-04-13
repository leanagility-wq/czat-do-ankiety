import json

import httpx

from app.config import get_settings


settings = get_settings()


class OpenAIClient:
    def __init__(self) -> None:
        self._base_url = settings.openai_base_url.rstrip("/")
        self._api_key = settings.openai_api_key
        self._model = settings.openai_model
        self._timeout = settings.openai_timeout_seconds

    @property
    def is_configured(self) -> bool:
        return bool(self._api_key)

    async def create_json_completion(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        schema_name: str,
        schema: dict,
    ) -> dict:
        payload = {
            "model": self._model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": schema_name,
                    "strict": True,
                    "schema": schema,
                },
            },
        }
        data = await self._post(payload)
        content = data["choices"][0]["message"]["content"]
        return json.loads(content)

    async def create_text_completion(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
    ) -> str:
        payload = {
            "model": self._model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        data = await self._post(payload)
        return data["choices"][0]["message"]["content"].strip()

    async def _post(self, payload: dict) -> dict:
        if not self._api_key:
            raise RuntimeError("OPENAI_API_KEY is not configured.")

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.post(
                f"{self._base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )
            response.raise_for_status()
            return response.json()
