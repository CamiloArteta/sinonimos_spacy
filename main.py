from fastapi import FastAPI
from routes import router

app = FastAPI(
    title = "Synonyms API",
    description = """Generador de sinónimos basado en términos relacionados con el sector
    de los muebles y la decoración"""
)

app.include_router(router.router, prefix="/insert", tags=["Sinonimos"])

@app.get("/")
def home():
    return {"message": "API sinónimos"}