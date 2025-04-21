import os
import asyncio
from dotenv import load_dotenv
from langchain_community.llms import Ollama
from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate

# Загрузка переменных окружения
load_dotenv()

class LLMProcessor:
    def __init__(self):
        ollama_host = os.getenv("OLLAMA_HOST", "http://localhost:11434")
        self.llm = Ollama(
            base_url=ollama_host,
            model="llama3.2",
            temperature=0.7,
            top_p=0.9
        )
        
        self.prompt_template = PromptTemplate(
            template="""
            Анализируй контент и отвечай на вопрос. Будь точным и используй только предоставленные данные.
            Если не найдешь информацию в тексте, ответь на основании своих знаний или спроси дополнительный вопрос.
            
            Контент:
            {data}
            
            Вопрос: {question}
            
            Ответ:
            """,
            input_variables=["data", "question"]
        )
        
        self.chain = LLMChain(
            llm=self.llm,
            prompt=self.prompt_template
        )

    async def process_query(self, data: str, question: str) -> str:
        """Асинхронная обработка запроса через LLM"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            lambda: self.chain.run(data=data, question=question)
        )
