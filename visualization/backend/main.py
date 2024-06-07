import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from src.router.charts import router as chart_router

from fastapi import FastAPI

app = FastAPI()
app.include_router(router=chart_router)