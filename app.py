from fastapi import FastAPI, Query
from databricks import sql
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
import os
from datetime import datetime, date
from decimal import Decimal

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "¡Bienvenido a la API de Databricks! Usa /api/stock para obtener datos."}

