import asyncio
import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from senior_engineering_agent import SeniorEngineeringAgent
from mariner_expert_agent import MarinerExpertAgent
from cloud_engineer_agent import CloudEngineerAgent

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("Orchestrator")

# Load environment variables from .env file (like GOOGLE_API_KEY)
from dotenv import load_dotenv
load_dotenv()

# ==========================================
# INTERFACES (The Flexible Framework)
# ==========================================

class DataSource(ABC):
    """Handles reading tasks from and writing status back to the database (e.g., Google Sheets)."""
    @abstractmethod
    async def fetch_pending_tasks(self) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    async def update_task_status(self, task_id: str, status: str, error_message: str = "") -> None:
        pass

class DataGatherer(ABC):
    """Handles scraping or fetching raw data from external sources (0 token cost)."""
    @abstractmethod
    async def gather(self, source_info: str) -> str:
        pass

class MicroAgent(ABC):
    """Handles structured LLM calls (e.g., Extraction, Copywriting)."""
    @abstractmethod
    async def process(self, input_data: str, instructions: Dict[str, Any]) -> Dict[str, Any]:
        pass

class Publisher(ABC):
    """Handles pushing the final content to the destination (e.g., Google Business Profile API)."""
    @abstractmethod
    async def publish(self, content: Dict[str, Any], destination_id: str) -> bool:
        pass

# ==========================================
# CORE ORCHESTRATOR
# ==========================================

class SwarmOrchestrator:
    """
    The central intelligence that coordinates the microagents and scripts asynchronously.
    Designed for 10x efficiency via concurrent processing and batched data aggregation.
    """
    def __init__(
        self,
        data_source: DataSource,
        gatherer: DataGatherer,
        extractor: MicroAgent,
        copywriter: MicroAgent,
        publisher: Publisher,
        max_concurrency: int = 10
    ):
        self.data_source = data_source
        self.gatherer = gatherer
        self.extractor = extractor
        self.copywriter = copywriter
        self.publisher = publisher
        # Semaphore limits how many concurrent tasks run to avoid rate limits
        self.semaphore = asyncio.Semaphore(max_concurrency)

    async def _process_single_task(self, task: Dict[str, Any]):
        """Executes the microagent pipeline for a single post task."""
        task_id = task.get("id", "unknown_id")
        destination_id = task.get("destination_id", "unknown_location")
        source_url = task.get("source_url", "")
        topic = task.get("topic", "General Update")
        
        try:
            logger.info(f"[Task {task_id}] Began processing for destination: {destination_id}")

            # 1. GATHER: Fetch raw data efficiently via free tools (BeautifulSoup, APIs, etc)
            raw_text = await self.gatherer.gather(source_url)
            if not raw_text:
                raise ValueError("DataGatherer failed to retrieve content.")

            # 2. EXTRACT: Condense the raw info into a focused JSON (Low token usage)
            extraction_instructions = {"topic": topic, "focus": "key facts and updates"}
            extracted_facts = await self.extractor.process(raw_text, extraction_instructions)

            # 3. WRITE: Generate the final Google Post copy based heavily on the extracted JSON
            copy_instructions = {"call_to_action": task.get("cta", "Learn More"), "tone": task.get("tone", "Professional")}
            # We convert extracted_facts to string for the microagent input
            final_post = await self.copywriter.process(str(extracted_facts), copy_instructions)

            # 4. PUBLISH: Push to Google Business Profile API
            is_success = await self.publisher.publish(final_post, destination_id)

            if is_success:
                # 5. LOG: Update Google Sheet successfully
                await self.data_source.update_task_status(task_id, "Posted")
                logger.info(f"[Task {task_id}] Successfully posted to GBP.")
            else:
                raise RuntimeError("Publisher rejected the API payload.")

        except Exception as e:
            logger.error(f"[Task {task_id}] Failed at pipeline hurdle: {e}")
            await self.data_source.update_task_status(task_id, "Failed", str(e))

    async def run_cycle(self):
        """
        The main loop. Pulls tasks and processes them highly concurrently.
        This provides the 10x speed boost compared to synchronous loops.
        """
        logger.info("Initializing Swarm Cycle...")
        tasks = await self.data_source.fetch_pending_tasks()
        
        if not tasks:
            logger.info("No pending tasks to process.")
            return

        logger.info(f"Loaded {len(tasks)} tasks. Launching concurrent processing...")

        async def _bounded_execution(task):
            async with self.semaphore:
                await self._process_single_task(task)

        # asyncio.gather fires all tasks simultaneously, constrained only by the semaphore
        await asyncio.gather(*(_bounded_execution(t) for t in tasks))
        
        logger.info("Swarm Cycle complete.")

# ==========================================
# MOCK IMPLEMENTATIONS (For immediate testing)
# ==========================================

class MockSheetsSource(DataSource):
    async def fetch_pending_tasks(self):
        await asyncio.sleep(0.5) # Simulate API latency
        return [
            {"id": "1", "destination_id": "LOC_123", "source_url": "https://client.com/updates", "topic": "New Hours", "cta": "Visit Us"},
            {"id": "2", "destination_id": "LOC_456", "source_url": "https://client2.com/news", "topic": "Product Launch", "cta": "Buy Now"},
        ]
    async def update_task_status(self, task_id, status, error_message=""):
        await asyncio.sleep(0.1)
        logger.info(f"SHEET UPDATE: Task {task_id} -> {status}")

class MockScraper(DataGatherer):
    async def gather(self, source_info):
        await asyncio.sleep(1) # Simulate network request
        return f"<html><body>Raw text found at {source_info}: They are open till 9PM now.</body></html>"

class MockExtractorAgent(MicroAgent):
    async def process(self, input_data, instructions):
        await asyncio.sleep(0.5) # Simulate fast LLM inference
        return {"extracted_facts": ["Open until 9PM"], "topic": instructions['topic']}

class MockCopywriterAgent(MicroAgent):
    async def process(self, input_data, instructions):
        await asyncio.sleep(0.5) # Simulate fast LLM inference
        return {"post_body": "Great news! We have new hours. We're now open until 9PM.", "media": None, "action_type": instructions.get("call_to_action")}

class MockGBPAPI(Publisher):
    async def publish(self, content, destination_id):
        await asyncio.sleep(0.8) # Simulate GBP API integration
        logger.info(f"API CALL -> Published to {destination_id}: {content['post_body']}")
        return True

if __name__ == "__main__":
    async def main():
        # Assembly line construction
        orchestrator = SwarmOrchestrator(
            data_source=MockSheetsSource(),
            gatherer=MockScraper(),
            extractor=MockExtractorAgent(),
            copywriter=MockCopywriterAgent(),
            publisher=MockGBPAPI(),
            max_concurrency=5 # Process 5 posts at a time safely
        )
        
        # Instantiate the specialized agents for specific tasks
        senior_engineer = SeniorEngineeringAgent()
        mariner_expert = MarinerExpertAgent()
        cloud_engineer = CloudEngineerAgent()
        
        logger.info("Senior Engineering MicroAgent initialized.")
        logger.info("Project Mariner Expert MicroAgent initialized.")
        logger.info("Cloud Architect (Bearded, IPA-drinking) MicroAgent initialized.")

        # Fire the Orchestrator
        await orchestrator.run_cycle()

    # Python 3.7+ async entry point
    asyncio.run(main())
