import os
import psycopg2
import pandas as pd
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain.schema import Document
from langchain_community.vectorstores import Chroma
from dotenv import load_dotenv
import chromadb
from chromadb.config import Settings

load_dotenv()


conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    port=int(os.getenv("POSTGRES_PORT")),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)

df = pd.read_sql("SELECT * FROM argo_data_test", conn)
conn.close()

documents = []
for _, row in df.iterrows():
    # Convert metadata to dict and cast timestamps to string
    metadata = {}
    for k, v in row.to_dict().items():
        if isinstance(v, (pd.Timestamp, pd.DatetimeTZDtype)):
            metadata[k] = str(v)
        else:
            metadata[k] = v
    
    text = " | ".join([f"{col}: {metadata[col]}" for col in df.columns])
    documents.append(Document(page_content=text, metadata=metadata))

embedding_model = GoogleGenerativeAIEmbeddings(model="models/embedding-001")

settings = Settings(
    chroma_server_host=os.getenv('CHROMA_HOST'),
    chroma_server_http_port=os.getenv('CHROMA_PORT',8000)
)

client = chromadb.Client(settings=settings)
collection = client.get_or_create_collection("argo_test_chunks")

embeddings = [
    embedding_model.embed_query(doc.page_content) for doc in documents
]

collection.add(
    ids=[f"row_{i}" for i in range(len(documents))],
    documents=[doc.page_content for doc in documents],
    embeddings=embeddings,
    metadatas=[doc.metadata for doc in documents]
)

print("Embeddings created!")
collection = client.get_collection("argo_test_chunks")
print("Number of stored embeddings:", collection.count())
