from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app import models  # noqa: F401
from app.config import get_settings
from app.db import Base, engine, get_db_session
from app.schemas import ChatRequest, ChatResponse, ExampleQuestion, HealthResponse
from app.services.chat import answer_question
from app.web import render_index_html


settings = get_settings()


@asynccontextmanager
async def lifespan(_: FastAPI):
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    yield


app = FastAPI(title=settings.app_name, debug=settings.debug, lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")

EXAMPLE_QUESTIONS = [
    "Jakie obawy najczęściej zgłaszali Scrum Masterzy?",
    "Czy częstsze używanie AI wiąże się z większą efektywnością?",
    "W jakie kompetencje badani chcą inwestować?",
    "Jakie były pytania otwarte?",
    "Jakie odpowiedzi były możliwe przy pytaniu o częstotliwość używania AI?",
]


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    return render_index_html(EXAMPLE_QUESTIONS)


@app.get("/health", response_model=HealthResponse)
async def health(session: AsyncSession = Depends(get_db_session)) -> HealthResponse:
    await session.execute(text("SELECT 1"))
    return HealthResponse(status="ok", app_name=settings.app_name)


@app.get("/examples", response_model=list[ExampleQuestion])
async def examples() -> list[ExampleQuestion]:
    return [ExampleQuestion(question=question) for question in EXAMPLE_QUESTIONS]


@app.post("/chat", response_model=ChatResponse)
async def chat(
    payload: ChatRequest,
    session: AsyncSession = Depends(get_db_session),
) -> ChatResponse:
    return await answer_question(payload.question, session)
