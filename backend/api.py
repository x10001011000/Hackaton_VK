from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from llm_integration import LLMProcessor
from site_search import SiteSearchEngine
import orjson
import asyncio

app = FastAPI()
search_engine = SiteSearchEngine(DB_CONFIG)
llm_processor = LLMProcessor()

@app.post("/api/chat")
async def chat_handler(request: ChatRequest):
    async def generate_response():
        async for chunk in search_engine.get_site_content(request.site_name):
            # Обработка каждого чанка через LLM
            processed = await llm_processor.process_query(
                data=chunk["content"],
                question=request.question
            )
            
            yield orjson.dumps({
                "content": processed,
                "metadata": chunk["metadata"],
                "source": chunk["metadata"]["type"]
            }) + b"\n"
    
    return StreamingResponse(generate_response())
