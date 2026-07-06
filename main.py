import os
import asyncio
import pandas as pd
import httpx
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response

app = FastAPI(title="Ingestion Service", version="1.0.0")

# Prometheus Metrics
SENT_COUNT = Counter("ingestion_sent_total", "Total messages ingested and sent to gateway")
SIMULATION_ACTIVE = Gauge("ingestion_simulation_active", "State of the simulation stream")

# Global variables for simulation control
SIM_TASK = None
SIM_RUNNING = False
CURRENT_ROW = 0
DATA_PATH = "/app/datasets/processed/veremi_subset.csv"
VALIDATION_SERVICE_URL = os.getenv("VALIDATION_SERVICE_URL", "http://validation-service:8002")

@app.on_event("startup")
def verify_dataset():
    global DATA_PATH
    if not os.path.exists(DATA_PATH):
        # Fallback for local testing
        DATA_PATH = "datasets/processed/veremi_subset.csv"
    if not os.path.exists(DATA_PATH):
        print(f"Warning: Dataset not found at {DATA_PATH}. Please run preprocessing.")

async def stream_data():
    global SIM_RUNNING, CURRENT_ROW, DATA_PATH, VALIDATION_SERVICE_URL
    SIMULATION_ACTIVE.set(1)
    
    if not os.path.exists(DATA_PATH):
        print("Error: Dataset file not found. Terminating stream.")
        SIM_RUNNING = False
        SIMULATION_ACTIVE.set(0)
        return
        
    print(f"Loading dataset from {DATA_PATH}...")
    df = pd.read_csv(DATA_PATH)
    total_rows = len(df)
    
    async with httpx.AsyncClient(timeout=5.0) as client:
        while SIM_RUNNING and CURRENT_ROW < total_rows:
            row = df.iloc[CURRENT_ROW]
            
            # Map CSV row to Unified Schema
            payload = {
                "message_id": f"msg_{CURRENT_ROW:06d}",
                "source_id": int(row["vehicle_id"]),
                "source_type": "v2x",
                "timestamp": float(row["rcvTime"]),
                "x_position": float(row["pos_0"]),
                "y_position": float(row["pos_1"]),
                "speed": float(row["speed"]),
                "heading": float(row["heading"]),
                "acceleration": float(row["acceleration"]),
                "message_type": "BSM" if row["type"] == 3 else "EGO_TELEMETRY",
                "freshness_value": int(row["freshness_value"]),
                "auth_tag_valid": bool(row["auth_tag_valid"]),
                "signature_valid": bool(row["signature_valid"]),
                "hash_valid": bool(row["hash_valid"]),
                "ota_version": str(row["ota_version"]),
                "label": "anomaly" if row["attack"] == 1 else "normal",
                "attack_type": str(row["attack_type"])
            }
            
            try:
                # Forward to Validation Service
                url = f"{VALIDATION_SERVICE_URL}/validate"
                response = await client.post(url, json=payload)
                if response.status_code == 200:
                    SENT_COUNT.inc()
                else:
                    print(f"Validation Service returned status {response.status_code}")
            except Exception as e:
                print(f"Failed to transmit packet to validation service: {e}")
                
            CURRENT_ROW += 1
            # Adjust sleep to control stream rate (e.g. 0.1s is 10 messages/sec)
            await asyncio.sleep(0.05)
            
    SIM_RUNNING = False
    SIMULATION_ACTIVE.set(0)

@app.post("/simulation/start")
def start_simulation(background_tasks: BackgroundTasks):
    global SIM_RUNNING, SIM_TASK
    if SIM_RUNNING:
        return {"status": "already_running", "row": CURRENT_ROW}
        
    SIM_RUNNING = True
    background_tasks.add_task(stream_data)
    return {"status": "started", "row": CURRENT_ROW}

@app.post("/simulation/stop")
def stop_simulation():
    global SIM_RUNNING
    if not SIM_RUNNING:
        return {"status": "not_running", "row": CURRENT_ROW}
        
    SIM_RUNNING = False
    return {"status": "stopped", "row": CURRENT_ROW}

@app.post("/simulation/reset")
def reset_simulation():
    global CURRENT_ROW, SIM_RUNNING
    SIM_RUNNING = False
    CURRENT_ROW = 0
    return {"status": "reset", "row": 0}

@app.get("/simulation/status")
def get_status():
    global SIM_RUNNING, CURRENT_ROW
    return {
        "running": SIM_RUNNING,
        "current_row": CURRENT_ROW,
        "target_url": VALIDATION_SERVICE_URL
    }

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/health")
def health():
    return {"status": "healthy"}
