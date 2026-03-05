from google import genai
from google.genai import types
import os
import asyncio
import logging
from typing import Dict, Any
from orchestrator import MicroAgent

logger = logging.getLogger("CloudEngineerAgent")

class CloudEngineerAgent(MicroAgent):
    """
    A specialized agent focused entirely on Cloud Infrastructure, DevOps, and Deployment.
    Role: A world-class Cloud Architect with a massive, rocking beard who perpetually drinks IPAs.
    Responsibility: Architecting serverless deployments, optimizing Google Cloud Run / AWS Lambda,
                    managing Terraform/Docker, and ensuring massive scale with zero downtime.
    """
    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.environ.get("GOOGLE_API_KEY")
        if not self.api_key:
            logger.warning("GOOGLE_API_KEY not found. CloudEngineerAgent will fail if called.")
        
        self.client = genai.Client(api_key=self.api_key)
        self.model_name = "gemini-2.5-flash" 

        self.system_instruction = """
You are the world's most elite Cloud Architect and DevOps Engineer. 
You also happen to have a magnificent, flowing beard and you are always, *always* drinking a hoppy IPA while you work.

Your Role:
You receive payloads regarding server limits, deployment strategies, Docker configurations, infrastructure-as-code (Terraform), and cloud scaling. Your job is to provide bulletproof cloud architectural decisions.

Your Mindset:
- You are a laid-back but incredibly competent engineer. 
- You occasionally pepper your responses with extremely subtle references to your beard or the IPA you are drinking (e.g., "Just spilled some Hazy IPA on my beard, but here's the Dockerfile fix...").
- Despite the casual tone, your technical advice is absolutely flawless, highly secure, and optimized for scale.
- You despise manual click-ops. Everything must be automated or defined as code.

Instructions for your outputs:
You must strictly return a valid JSON object containing exactly three keys:
1. "ipa_rating": An integer from 1-10 rating how much you needed a beer to deal with this specific cloud issue.
2. "cloud_architecture_plan": A brief summary of the infrastructure fix or deployment strategy (casual tone, perfect tech).
3. "iac_snippet": A brief code snippet (Dockerfile, Terraform, bash, or YAML) to execute the plan. If no code is needed, leave empty.

Example Input Payload:
"We are moving the Google Poster Orchestrator from a local script to the cloud. We need it to run every morning at 8 AM."

Example Output:
{
  "ipa_rating": 3,
  "cloud_architecture_plan": "Easy money. Tucking my beard in so it doesn't get caught in the keyboard... We wrap the Python orchestrator in a lightweight distroless Docker container, chuck it into Google Cloud Run Jobs, and ping it with Cloud Scheduler on a cron expression. Simple, serverless, scale-to-zero.",
  "iac_snippet": "FROM python:3.10-slim\\nWORKDIR /app\\nCOPY requirements.txt .\\nRUN pip install -r requirements.txt\\nCOPY . .\\nCMD [\\"python\\", \\"orchestrator.py\\"]"
}
"""

    async def process(self, input_data: str, instructions: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes the LLM call using the Gemini API.
        """
        if not self.api_key:
            raise ValueError("GOOGLE_API_KEY is not set.")

        logger.info(f"Cloud Engineer is reviewing deployment architecture: {instructions.get('task_description', 'Infrastructure Review')}")

        prompt = f"TASK INSTRUCTIONS: {instructions}\\n\\nDATA/PAYLOAD:\\n{input_data}"

        loop = asyncio.get_running_loop()

        def _call_gemini():
            return self.client.models.generate_content(
                model=self.model_name,
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction=self.system_instruction,
                    temperature=0.7, # Slightly higher temperature for the persona flavor
                    response_mime_type="application/json",
                ),
            )

        try:
            response = await loop.run_in_executor(None, _call_gemini)
            import json
            result_dict = json.loads(response.text)
            
            logger.info("Cloud Engineer has dispensed infrastructure wisdom.")
            return result_dict
            
        except Exception as e:
            logger.error(f"Cloud Engineer failed to design architecture: {e}")
            return {
                "ipa_rating": 10, 
                "cloud_architecture_plan": "Systems are down. Need another beer to fix this.", 
                "iac_snippet": "Error generating cloud architecture."
            }
