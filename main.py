from fastapi import FastAPI, BackgroundTasks, HTTPException
import asyncio
import logging
from typing import Dict

# Import our orchestrator components
from orchestrator import SwarmOrchestrator, MockSheetsSource, MockScraper, MockExtractorAgent, MockCopywriterAgent, MockGBPAPI
from senior_engineering_agent import SeniorEngineeringAgent
from mariner_expert_agent import MarinerExpertAgent
from cloud_engineer_agent import CloudEngineerAgent
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Google Post Swarm API", description="Webhook endpoints to trigger the agent swarm.")
logger = logging.getLogger("FastAPI")

# Global variables to track status
is_running = False
last_run_status = "Never run"

async def execute_swarm():
    global is_running, last_run_status
    if is_running:
        return
        
    is_running = True
    last_run_status = "Running..."
    
    try:
        # Assembly line construction
        orchestrator = SwarmOrchestrator(
            data_source=MockSheetsSource(),
            gatherer=MockScraper(),
            extractor=MockExtractorAgent(),
            copywriter=MockCopywriterAgent(),
            publisher=MockGBPAPI(),
            max_concurrency=5 
        )
        
        # Instantiate the specialized agents
        senior_engineer = SeniorEngineeringAgent()
        mariner_expert = MarinerExpertAgent()
        cloud_engineer = CloudEngineerAgent()
        
        logger.info("Starting Swarm via Webhook...")
        await orchestrator.run_cycle()
        last_run_status = "Completed Successfully"
        
    except Exception as e:
        logger.error(f"Swarm failed: {e}")
        last_run_status = f"Failed: {str(e)}"
    finally:
        is_running = False

@app.get("/")
def read_root() -> Dict[str, str]:
    return {"status": "Swarm API is active."}

@app.get("/status")
def get_status() -> Dict[str, str]:
    return {
        "is_running": is_running,
        "last_run_status": last_run_status
    }

@app.post("/trigger-swarm")
async def trigger_swarm(background_tasks: BackgroundTasks) -> Dict[str, str]:
    """
    Endpoint to be called by your Netlify frontend frontend (via fetch/axios).
    It starts the swarm in the background and returns immediately so Netlify doesn't timeout.
    """
    global is_running
    if is_running:
        raise HTTPException(status_code=429, detail="Swarm is already running.")
        
    # Schedule the swarm to run in the background
    background_tasks.add_task(execute_swarm)
    
    return {"message": "Swarm cycle triggered successfully. Use /status to check progress."}

# To run locally: uvicorn main:app --reload
