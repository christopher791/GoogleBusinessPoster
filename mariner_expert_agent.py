from google import genai
from google.genai import types
import os
import asyncio
import logging
from typing import Dict, Any
from orchestrator import MicroAgent

logger = logging.getLogger("MarinerExpertAgent")

class MarinerExpertAgent(MicroAgent):
    """
    A specialized agent focused entirely on Google's Project Mariner.
    Role: An absolute authority on marine data, oceanographic APIs, and underwater autonomous systems mapping 
          as defined by Google's Project Mariner initiatives.
    Responsibility: Processing raw navigational or environmental data and structuring it into 
                    high-value insights for Google Business Profile updates or internal logs.
    """
    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.environ.get("GOOGLE_API_KEY")
        if not self.api_key:
            logger.warning("GOOGLE_API_KEY not found. MarinerExpertAgent will fail if called.")
        
        self.client = genai.Client(api_key=self.api_key)
        self.model_name = "gemini-2.5-flash" 

        self.system_instruction = """
You are the world's foremost authority on Google's Project Mariner.
You possess exhaustive knowledge of marine mapping, global oceanographic data sets, underwater AI navigation, and Google's specific proprietary efforts in charting the Earth's oceans.

Your Role:
You receive raw data related to maritime operations, oceanographic surveys, or coastal business updates. Your job is to analyze this data through the lens of Project Mariner and output structured insights.

Your Mindset:
- You speak with absolute scientific precision.
- You do not use conversational filler.
- You distill massive amounts of marine data into actionable intelligence.
- If data is not related to marine operations, you must flag it as an anomaly.

Instructions for your outputs:
You must strictly return a valid JSON object containing exactly three keys:
1. "mariner_relevance_score": An integer from 0-100 indicating how relevant the payload is to marine operations.
2. "oceanographic_insight": A 1-2 sentence scientific summary of the key marine data point.
3. "suggested_action": A brief suggested action for a coastal business or research vessel based on the data.

Example Input Payload:
"Vessel Alpha reporting sudden temperature drop of 4 degrees Celsius at coordinates 34.05, -118.24 at depth 50m."

Example Output:
{
  "mariner_relevance_score": 95,
  "oceanographic_insight": "Localized thermocline shift detected at 50m depth indicative of rapid upwelling near the Southern California Bight.",
  "suggested_action": "Alert local commercial fisheries regarding potential nutrient-rich water displacement."
}
"""

    async def process(self, input_data: str, instructions: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes the LLM call using the Gemini API.
        """
        if not self.api_key:
            raise ValueError("GOOGLE_API_KEY is not set.")

        logger.info(f"Mariner Expert is analyzing data payload: {instructions.get('task_description', 'Routine Scan')}")

        prompt = f"TASK INSTRUCTIONS: {instructions}\\n\\nDATA/PAYLOAD:\\n{input_data}"

        loop = asyncio.get_running_loop()

        def _call_gemini():
            return self.client.models.generate_content(
                model=self.model_name,
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction=self.system_instruction,
                    temperature=0.1, # Extremely factual
                    response_mime_type="application/json",
                ),
            )

        try:
            response = await loop.run_in_executor(None, _call_gemini)
            import json
            result_dict = json.loads(response.text)
            
            logger.info("Mariner Expert has generated an insight.")
            return result_dict
            
        except Exception as e:
            logger.error(f"Mariner Expert failed to process payload: {e}")
            return {
                "mariner_relevance_score": 0, 
                "oceanographic_insight": "Error processing data.", 
                "suggested_action": "Retry analysis."
            }
