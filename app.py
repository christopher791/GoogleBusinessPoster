import streamlit as st
import asyncio
import logging
from io import StringIO
import sys

# Import our orchestrator components
from orchestrator import SwarmOrchestrator, MockSheetsSource, MockScraper, MockExtractorAgent, MockCopywriterAgent, MockGBPAPI
from senior_engineering_agent import SeniorEngineeringAgent
from mariner_expert_agent import MarinerExpertAgent
from cloud_engineer_agent import CloudEngineerAgent

st.set_page_config(page_title="Agent Swarm Dashboard", page_icon="🤖", layout="wide")

st.title("Google Post Agent Swarm Dashboard 🤖")
st.markdown("Monitor and control your multi-agent architecture from this control panel.")

# Setup an in-memory string stream to capture logs
if "log_stream" not in st.session_state:
    st.session_state.log_stream = StringIO()

# Setup a custom logging handler
class StreamlitLogHandler(logging.Handler):
    def __init__(self, st_placeholder):
        super().__init__()
        self.st_placeholder = st_placeholder

    def emit(self, record):
        log_entry = self.format(record)
        st.session_state.log_stream.write(log_entry + "\\n")
        self.st_placeholder.text(st.session_state.log_stream.getvalue())

# Function to run the swarm asynchronously
async def run_swarm(log_placeholder):
    # Retrieve the root logger
    logger = logging.getLogger()
    
    # Remove all handlers to clear them
    logger.handlers.clear()
    
    # Add Streamlit handler
    st_handler = StreamlitLogHandler(log_placeholder)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s', datefmt='%H:%M:%S')
    st_handler.setFormatter(formatter)
    logger.addHandler(st_handler)
    logger.setLevel(logging.INFO)
    
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
    
    logger.info("Initializing Agent Swarm System via Dashboard...")
    logger.info("Senior Engineering MicroAgent initialized.")
    logger.info("Project Mariner Expert MicroAgent initialized.")
    logger.info("Cloud Architect (Bearded, IPA-drinking) MicroAgent initialized.")
    
    # Fire the Orchestrator
    try:
        await orchestrator.run_cycle()
        st.success("Swarm Cycle Completed Successfully!", icon="✅")
    except Exception as e:
        st.error(f"Swarm encountered an error: {e}", icon="❌")
        logger.error(f"Error: {e}")

col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("Control Panel")
    st.metric(label="Active Agents", value="5")
    st.metric(label="Max Concurrency", value="5")
    
    if st.button("🚀 Execute Swarm Cycle", use_container_width=True, type="primary"):
        st.session_state.log_stream.truncate(0)
        st.session_state.log_stream.seek(0)
        
        log_placeholder = col2.empty()
        with st.spinner("Swarm is actively working..."):
            asyncio.run(run_swarm(log_placeholder))
    
    st.markdown("---")
    st.caption("Using Mock endpoints until Production Credentials are provided in .env")

with col2:
    st.subheader("Live System Logs")
    # Display the current logs (or empty container if not run yet)
    log_box = st.empty()
    if st.session_state.log_stream.getvalue():
        log_box.text(st.session_state.log_stream.getvalue())
    else:
        log_box.info("System idle. Logs will appear here during execution.")
