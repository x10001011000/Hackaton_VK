from langchain_community.llms import Ollama
from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate

# Подключение к Ollama через ngrok
llm = Ollama(base_url=ollama_host, model="llama3.2")

# Шаблон промпта
template = """
Проанализируй весь текст с html страниц и ответь на вопрос,
учитывая эти данные. Если не найдешь информацию в тексте, ответь
на основании своих знаний или спроси дополнительный вопрос:
{data}

Вопрос: {question}
"""

prompt = PromptTemplate(template=template, input_variables=["data", "question"])
chain = LLMChain(llm=llm, prompt=prompt)
