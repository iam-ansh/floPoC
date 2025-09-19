import os
from dotenv import load_dotenv
from langchain_chroma import Chroma
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from chromadb.config import Settings
from google import genai 

load_dotenv()


GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

embedding_model = GoogleGenerativeAIEmbeddings(
    model="models/embedding-001",
    google_api_key=GOOGLE_API_KEY
)

chroma_settings = Settings(
    chroma_server_host=os.getenv("CHROMA_HOST"),
    chroma_server_http_port=int(os.getenv("CHROMA_PORT", 8000))
)

vector_db = Chroma(
    collection_name="argo_collection_test",
    embedding_function=embedding_model,
    client_settings=chroma_settings
)


query = input("User: ")
search_results = vector_db.similarity_search(query=query)


context = "\n\n\n\n".join([
    f"""Data Chunk: {" | ".join([f"{col}: {doc.metadata.get(col)}" for col in doc.metadata.keys()])}
    Start Time: {doc.metadata.get('time_start')}
    End Time: {doc.metadata.get('time_end')}"""
    for doc in search_results
])

# SYSTEM PROMPT passed to LLM
SYSTEM_PROMPT = f"""
You are a helpful AI assistant answering queries about Argo oceanographic data.
Use ONLY the data provided in the context below. Each chunk contains row data with time ranges.
Provide insights, trends, or summaries based on this data.
Do NOT invent information outside the context.

Context:{context}
"""

client = genai.Client(api_key=GOOGLE_API_KEY)

res = client.models.generate_content(
    model="gemini-2.0-flash",
    contents=[
        {"role": "user", "parts": [{"text": SYSTEM_PROMPT}]},
        {"role": "user", "parts": [{"text": query}]}
    ]
)


print("\nFlo:", res.text)
