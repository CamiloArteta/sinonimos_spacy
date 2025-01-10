from fastapi import APIRouter
from fastapi import Body
from services import insert_service

router = APIRouter()

@router.post('/entrada')
def insert_data():
    insert_service.insert_data()
    return {"message": "Datos insertados"}

@router.post('/muestra')
def muestra(entrada: str = Body()):
    print('Esto es una muestra', entrada)