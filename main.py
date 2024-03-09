import json
from typing import Set, Dict, List
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Depends
from sqlalchemy import (
    create_engine,
    MetaData,
    Integer,
    String,
    Float,
    DateTime,
)
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.sql import select
from datetime import datetime
from pydantic import BaseModel, field_validator
from config import DATABASE_URL
from sqlalchemy import DateTime, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass

class ProcessedAgentDataDB(Base):
    __tablename__ = "processed_agent_data"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    road_state: Mapped[str] = mapped_column(String, index=True)
    user_id: Mapped[int] = mapped_column(Integer, index=True)
    x: Mapped[float] = mapped_column(Float, index=True)
    y: Mapped[float] = mapped_column(Float, index=True)
    z: Mapped[float] = mapped_column(Float, index=True)
    latitude: Mapped[float] = mapped_column(Float, index=True)
    longitude: Mapped[float] = mapped_column(Float, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, index=True)

# FastAPI app setup
app = FastAPI()

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base.metadata.create_all(bind=engine)
metadata = MetaData()


def get_database():
    database = SessionLocal()
    yield database
    database.close()


# FastAPI models
class AccelerometerData(BaseModel):
    x: float
    y: float
    z: float


class GpsData(BaseModel):
    latitude: float
    longitude: float


class AgentData(BaseModel):
    user_id: int
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


# WebSocket subscriptions
subscriptions: Dict[int, Set[WebSocket]] = {}


# FastAPI WebSocket endpoint
@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: int):
    await websocket.accept()
    if user_id not in subscriptions:
        subscriptions[user_id] = set()
    subscriptions[user_id].add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        subscriptions[user_id].remove(websocket)


# Function to send data to subscribed users
async def send_data_to_subscribers(user_id: int, data):
    if user_id in subscriptions:
        for websocket in subscriptions[user_id]:
            await websocket.send_json(json.dumps(data))


# FastAPI CRUDL endpoints
# POST request
@app.post("/processed_agent_data/")
async def create_processed_agent_data(
    data: List[ProcessedAgentData], database: Session = Depends(get_database)
):
    # Insert data to database
    # Send data to subscribers
    for item in data:
        db_item = ProcessedAgentDataDB(
            road_state=item.road_state,
            user_id=item.agent_data.user_id,
            x=item.agent_data.accelerometer.x,
            y=item.agent_data.accelerometer.y,
            z=item.agent_data.accelerometer.z,
            latitude=item.agent_data.gps.latitude,
            longitude=item.agent_data.gps.longitude,
            timestamp=item.agent_data.timestamp,
        )
        database.add(db_item)
        database.commit()
        database.refresh(db_item)
        await send_data_to_subscribers(item.agent_data.user_id, item)

#GET BY ID request
@app.get("/processed_agent_data/{processed_agent_data_id}", 
         response_model=ProcessedAgentData,)
def read_processed_agent_data(
    processed_agent_data_id: int,
    database: Session = Depends(get_database),
):
    # Get data by id
    db_data = database.query(ProcessedAgentDataDB).filter(ProcessedAgentDataDB.id == processed_agent_data_id).first()
        
    # Constructing the ProcessedAgentData model response
    agent_data = AgentData(
        user_id=db_data.user_id,
        accelerometer=AccelerometerData(x=db_data.x, y=db_data.y, z=db_data.z),
        gps=GpsData(latitude=db_data.latitude, longitude=db_data.longitude),
        timestamp=db_data.timestamp
    )
    processed_data = ProcessedAgentData(road_state=db_data.road_state, agent_data=agent_data)
    
    return processed_data

#GET ALL
@app.get("/processed_agent_data/", response_model=list[ProcessedAgentData])
def list_processed_agent_data(database: Session = Depends(get_database)):
    # Get list of data
    db_data = database.query(ProcessedAgentDataDB).all()
    processed_data = []
    for db_item in db_data:
        agent_data = AgentData(
            user_id=db_item.user_id,
            accelerometer=AccelerometerData(x=db_item.x, y=db_item.y, z=db_item.z),
            gps=GpsData(latitude=db_item.latitude, longitude=db_item.longitude),
            timestamp=db_item.timestamp
        )
        processed_data.append(ProcessedAgentData(road_state=db_item.road_state, agent_data=agent_data))
    
    return processed_data

#PUT data
@app.put(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentData,
)
def update_processed_agent_data(
    processed_agent_data_id: int,
    data: ProcessedAgentData,
    database: Session = Depends(get_database),
):
    # Update data
    db_data = database.query(ProcessedAgentDataDB).filter(ProcessedAgentDataDB.id == processed_agent_data_id).first()
    if db_data is None:
        raise HTTPException(status_code=404, detail="Processed agent data not found")
    
    # Update the fields with new data
    db_data.road_state = data.road_state
    db_data.user_id = data.agent_data.user_id
    db_data.x = data.agent_data.accelerometer.x
    db_data.y = data.agent_data.accelerometer.y
    db_data.z = data.agent_data.accelerometer.z
    db_data.latitude = data.agent_data.gps.latitude
    db_data.longitude = data.agent_data.gps.longitude
    db_data.timestamp = data.agent_data.timestamp
    
    # Commit the changes to the database
    database.commit()
    
    # Constructing the ProcessedAgentData model response
    agent_data = AgentData(
        user_id=db_data.user_id,
        accelerometer=AccelerometerData(x=db_data.x, y=db_data.y, z=db_data.z),
        gps=GpsData(latitude=db_data.latitude, longitude=db_data.longitude),
        timestamp=db_data.timestamp
    )
    processed_data = ProcessedAgentData(road_state=db_data.road_state, agent_data=agent_data)
    
    return processed_data

#DELETE FROM AgentData
@app.delete(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentData,
)
def delete_processed_agent_data(
    processed_agent_data_id: int,
    database: Session = Depends(get_database),
):
    # Delete by id
    db_data = database.query(ProcessedAgentDataDB).filter(ProcessedAgentDataDB.id == processed_agent_data_id).first()
    if db_data is None:
        raise HTTPException(status_code=404, detail="Processed agent data not found")
    database.delete(db_data)
    database.commit()
    
    # Constructing the ProcessedAgentData model response for the deleted data
    agent_data = AgentData(
        user_id=db_data.user_id,
        accelerometer=AccelerometerData(x=db_data.x, y=db_data.y, z=db_data.z),
        gps=GpsData(latitude=db_data.latitude, longitude=db_data.longitude),
        timestamp=db_data.timestamp
    )
    processed_data = ProcessedAgentData(road_state=db_data.road_state, agent_data=agent_data)
    
    return processed_data

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)