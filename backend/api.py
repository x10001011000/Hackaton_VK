from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from llm_integration import LLMProcessor
from site_search import SiteSearchEngine
import orjson
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()
search_engine = SiteSearchEngine()
llm_processor = LLMProcessor()

# Модель запроса
class ChatRequest(BaseModel):
    site_name: str
    question: str

@app.post("/api/chat")
async def chat_handler(request: ChatRequest):
    async def generate_response():
        try:
            async for chunk in search_engine.get_site_content(request.site_name):
                processed = await llm_processor.process_query(
                    data=chunk["content"],
                    question=request.question
                )

                yield orjson.dumps({
                    "content": processed,
                    "metadata": chunk["metadata"],
                    "source": chunk["metadata"]["type"]
                }) + b"\n"
        except Exception as e:
            yield orjson.dumps({"error": str(e)})

    return StreamingResponse(generate_response(), media_type="application/json")
