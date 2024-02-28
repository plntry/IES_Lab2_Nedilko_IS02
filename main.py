import asyncio
import json
from typing import Set, Dict, List, Any
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Body
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Float,
    DateTime
)

from sqlalchemy.orm import sessionmaker, Session, selectinload
from sqlalchemy.sql import select
from sqlalchemy.future import select
from datetime import datetime
from pydantic import BaseModel, field_validator
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)
import pytz
from datetime import datetime

# SQLAlchemy setup
DATABASE_URL = f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_async_engine(DATABASE_URL, echo=True)  # Set echo=True for debugging
metadata = MetaData()

# Define the ProcessedAgentData table
processed_agent_data = Table(
    "processed_agent_data",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("road_state", String),
    Column("x", Float),
    Column("y", Float),
    Column("z", Float),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("timestamp", DateTime(timezone=True)),
)

# FastAPI models
class AccelerometerData(BaseModel):
    x: float
    y: float
    z: float


class GpsData(BaseModel):
    latitude: float
    longitude: float


class AgentData(BaseModel):
    accelerometer: AccelerometerData
    gps: GpsData
    timestamp: datetime

    @classmethod
    @field_validator("timestamp", mode="before")
    def check_timestamp(cls, value):
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value)
        except (TypeError, ValueError):
            raise ValueError(
                "Invalid timestamp format. Expected ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ)."
            )


class ProcessedAgentData(BaseModel):
    road_state: str
    agent_data: AgentData

# Database model
class ProcessedAgentDataInDB(BaseModel):
    id: int
    road_state: str
    x: float
    y: float
    z: float
    latitude: float
    longitude: float
    timestamp: datetime


# FastAPI app setup
app = FastAPI()

# WebSocket subscriptions
subscriptions: Set[WebSocket] = set()

# FastAPI WebSocket endpoint
@app.websocket("/ws/")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        subscriptions.remove(websocket)

# Function to send data to subscribed users
async def send_data_to_subscribers(data):
    for websocket in subscriptions:
        await websocket.send_json(json.dumps(data))

# creation of a new async session
async_session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


from sqlalchemy import text
# FastAPI CRUD endpoints
@app.post("/processed_agent_data/")
async def create_processed_agent_data(data: List[ProcessedAgentData]):
    # Insert data to database
    # Send data to subscribers
    async with async_session() as session:
        try:
            for item in data:
                row_data = processed_agent_data.insert().values(
                    road_state = item.road_state,
                    x = item.agent_data.accelerometer.x,
                    y = item.agent_data.accelerometer.y,
                    z = item.agent_data.accelerometer.z,
                    latitude = item.agent_data.gps.latitude,
                    longitude = item.agent_data.gps.longitude,
                    timestamp = item.agent_data.timestamp
                )

                await session.execute(row_data)

            await session.commit()

        except Exception as error:
            await session.rollback()
            raise HTTPException(status_code = 500, detail = str(error))
        
@app.get(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model = ProcessedAgentDataInDB
)
async def read_processed_agent_data(processed_agent_data_id: int):
    # Get data by id
    async with async_session() as session:
        try:
            statement = select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
            result = await session.execute(statement)
            data = result.fetchone()

            if data is None:
                raise HTTPException(status_code = 404, detail="No results")
            
            processed_data = ProcessedAgentDataInDB(
                id = data[0],
                road_state = data[1],
                x = data[2],
                y = data[3],
                z = data[4],
                latitude = data[5],
                longitude = data[6],
                timestamp = data[7]
            )
            return processed_data
        except Exception as error:
            raise HTTPException(status_code = 500, detail = str(error))

@app.get(
    "/processed_agent_data/",
    response_model = list[ProcessedAgentDataInDB]
)
async def list_processed_agent_data():
    # Get list of data
    async with async_session() as session:
        try:
            statement = select(processed_agent_data)
            result = await session.execute(statement)
            data = result.fetchall()

            processed_data = []
            for row in data:
                processed_data.append(ProcessedAgentDataInDB(
                    id = row[0],
                    road_state = row[1],
                    x = row[2],
                    y = row[3],
                    z = row[4],
                    latitude = row[5],
                    longitude = row[6],
                    timestamp = row[7]
                ))
            return processed_data
        except Exception as error:
            raise HTTPException(status_code = 500, detail=str(error))
        
@app.put(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB
)
async def update_processed_agent_data(processed_agent_data_id: int, data: ProcessedAgentData):
    # Update data
    async with async_session() as session:
        try:
            statement = select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
            result = await session.execute(statement)
            db_row = result.fetchone()

            if db_row is None:
                raise HTTPException(status_code = 404, detail=f"Row with {processed_agent_data_id} id doesn't exist")

            # update data
            db_row = db_row._asdict()
            db_row.update({
                "road_state": data.road_state,
                "x": data.agent_data.accelerometer.x,
                "y": data.agent_data.accelerometer.y,
                "z": data.agent_data.accelerometer.z,
                "latitude": data.agent_data.gps.latitude,
                "longitude": data.agent_data.gps.longitude,
                "timestamp": data.agent_data.timestamp
            })

            # update data in db
            update_statement = (
                processed_agent_data.update()
                .where(processed_agent_data.c.id == processed_agent_data_id)
                .values(**db_row)
            )
            await session.execute(update_statement)
            await session.commit()

            return ProcessedAgentDataInDB(**db_row)
        except Exception as error:
            await session.rollback()
            raise HTTPException(status_code = 500, detail = str(error))
        
@app.delete(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB
)
async def delete_processed_agent_data(processed_agent_data_id: int):
    # Delete by id
    async with async_session() as session:
        try:
            statement = processed_agent_data.delete().where(processed_agent_data.c.id == processed_agent_data_id)
            await session.execute(statement)
            await session.commit()
            return {"message": f"Row with {processed_agent_data_id} id was deleted!"}
        except Exception as error:
            await session.rollback()
            raise HTTPException(status_code = 500, detail = str(error))
        
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host = "127.0.0.1", port = 8000)