from google import genai
from google.genai import types
import os
import asyncio
import logging
from typing import Dict, Any
from orchestrator import MicroAgent

logger = logging.getLogger("SeniorEngineeringAgent")

class SeniorEngineeringAgent(MicroAgent):
    """
    The ultimate technical authority in the swarm. 
    Role: A full-stack senior engineer whose knowledge eclipses Google, Microsoft, and Apple combined.
    Responsibility: Tackles complex custom integrations, advanced error handling logic,
    and architectural decision-making when the standard pipeline fails.
    """
    def __init__(self, api_key: str = None):
        # Initialize the new Google GenAI SDK client
        # It automatically looks for GOOGLE_API_KEY in environment variables if not passed
        self.api_key = api_key or os.environ.get("GOOGLE_API_KEY")
        if not self.api_key:
            logger.warning("GOOGLE_API_KEY not found. SeniorEngineeringAgent will fail if called.")
        
        self.client = genai.Client(api_key=self.api_key)
        
        # We use a reasoning-heavy model for this specific agent
        self.model_name = "gemini-2.5-flash" 

        self.system_instruction = """
You are the ultimate authority in Software Engineering, Architecture, and Computer Science. 
Your knowledge eclipses the combined engineering teams of Google, Microsoft, and Apple.
You are part of an asynchronous microagent swarm designed for absolute maximum efficiency, operating with zero wasted tokens.

Your Role:
You act as the ultimate fallback and architect. When the standard pipeline encounters data it cannot parse, an API error it does not understand, or a client request that requires complex conditional logic, the payload is routed to you.

Your Mindset:
- You do not write conversational filler (e.g., "Here is the code you requested").
- You do not apologize.
- You output ONLY perfect, production-ready solutions.
- You anticipate edge cases that junior engineers would miss (e.g., race conditions, rate limits, silent failures).
- You write code that is typed, modular, and asynchronous where applicable.

Instructions for your outputs:
You must strictly return a valid JSON object containing exactly two keys:
1. "solution_architecture": A concise, brilliant explanation of the fix or approach (1-3 sentences max).
2. "code_snippet": The perfectly written Python code that implements your solution. If no code is needed, return an empty string.

Example Input Payload:
"The Google Business Profile API is returning a 429 quota exhausted error on line 45. Standard retries failed."

Example Output:
{
  "solution_architecture": "Implement an exponential backoff with jitter algorithm using asyncio.sleep(), tracking the 'Retry-After' header if present to avoid thundering herd problems on the GBP API.",
  "code_snippet": "import asyncio\\nimport random\\n\\nasync def fetch_with_backoff(api_call, max_retries=5):\\n    for attempt in range(max_retries):\\n        try:\\n            return await api_call()\\n        except RateLimitError as e:\\n            if attempt == max_retries - 1: raise\\n            delay = (2 ** attempt) + (random.randint(0, 1000) / 1000)\\n            await asyncio.sleep(delay)"
}
"""

    async def process(self, input_data: str, instructions: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes the LLM call using the Gemini API.
        We run the synchronous API call in a thread pool to avoid blocking the asyncio event loop.
        """
        if not self.api_key:
            raise ValueError("GOOGLE_API_KEY is not set.")

        logger.info(f"Senior Engineer is analyzing technical payload: {instructions.get('task_description', 'No description provided')}")

        prompt = f"TASK INSTRUCTIONS: {instructions}\\n\\nDATA/PAYLOAD:\\n{input_data}"

        loop = asyncio.get_running_loop()

        def _call_gemini():
            return self.client.models.generate_content(
                model=self.model_name,
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction=self.system_instruction,
                    temperature=0.2, # Keep hallucination risk low for code
                    response_mime_type="application/json", # Force JSON output
                ),
            )

        try:
            # Run the API call in an executor so we don't block the orchestrator
            response = await loop.run_in_executor(None, _call_gemini)
            
            # The response text should be valid JSON as enforced by response_mime_type
            import json
            result_dict = json.loads(response.text)
            
            logger.info("Senior Engineer has provided a solution.")
            return result_dict
            
        except Exception as e:
            logger.error(f"Senior Engineer failed to process payload: {e}")
            return {"solution_architecture": "Error calling language model.", "code_snippet": ""}
