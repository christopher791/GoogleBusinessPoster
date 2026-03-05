import asyncio
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from typing import List, Dict, Any
import logging
from orchestrator import DataSource

logger = logging.getLogger("GoogleSheetsSource")

class GoogleSheetsSource(DataSource):
    """
    Concrete implementation of DataSource using Google Sheets.
    Requires a service account JSON file and a Google Sheet shared with the service account email.
    """
    def __init__(self, credentials_path: str, sheet_url: str, worksheet_name: str = "Sheet1"):
        self.credentials_path = credentials_path
        self.sheet_url = sheet_url
        self.worksheet_name = worksheet_name
        self.client = self._authenticate()
        
        # Connect to the specific sheet and worksheet
        try:
            self.sheet = self.client.open_by_url(self.sheet_url).worksheet(self.worksheet_name)
            logger.info(f"Successfully connected to Google Sheet: {self.sheet_url}")
        except Exception as e:
            logger.error(f"Failed to connect to Google Sheet: {e}")
            raise

    def _authenticate(self):
        """Authenticates using a Service Account JSON file."""
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.credentials_path, scope)
        client = gspread.authorize(creds)
        return client

    async def fetch_pending_tasks(self) -> List[Dict[str, Any]]:
        """
        Reads the sheet and finds rows where 'Status' is 'Pending'.
        Using run_in_executor to make the synchronous gspread call non-blocking.
        """
        loop = asyncio.get_running_loop()
        
        # Run the synchronous API call in a thread pool
        def _get_records():
            return self.sheet.get_all_records()
            
        try:
            # gspread get_all_records automatically uses the first row as dictionary keys
            records = await loop.run_in_executor(None, _get_records)
            
            pending_tasks = []
            for idx, row in enumerate(records, start=2): # +2 because row 1 is header, 0-indexed list
                # Assuming the sheet has a 'Status' column
                if str(row.get('Status', '')).strip().lower() == 'pending':
                    task = {
                        "row_index": idx, # Keep track of row for updating later
                        "id": f"row_{idx}",
                        "destination_id": row.get('Location ID', ''),
                        "source_url": row.get('Source URL', ''),
                        "topic": row.get('Topic', ''),
                        "cta": row.get('CTA', 'Learn More'),
                        "tone": row.get('Tone', 'Professional')
                    }
                    pending_tasks.append(task)
            
            logger.info(f"Found {len(pending_tasks)} pending tasks in the sheet.")
            return pending_tasks
            
        except Exception as e:
            logger.error(f"Error fetching tasks from Google Sheets: {e}")
            return []

    async def update_task_status(self, task_id: str, status: str, error_message: str = "") -> None:
        """
        Updates the 'Status' column and 'Error Logs' column (if applicable) for a specific row.
        """
        loop = asyncio.get_running_loop()
        
        try:
            # Extract row index from task_id (e.g., "row_3" -> 3)
            row_index = int(task_id.split('_')[1])
            
            # Find the column index for 'Status' dynamically
            headers = await loop.run_in_executor(None, self.sheet.row_values, 1)
            
            try:
                status_col_index = headers.index('Status') + 1 # 1-indexed for gspread
            except ValueError:
                logger.error("'Status' column not found in sheet headers.")
                return
                
            # Update the status cell
            def _update_cell():
                self.sheet.update_cell(row_index, status_col_index, status)
                
                # Optionally update error message if there's an 'Error Logs' column
                if error_message and 'Error Logs' in headers:
                    error_col_index = headers.index('Error Logs') + 1
                    self.sheet.update_cell(row_index, error_col_index, error_message)

            await loop.run_in_executor(None, _update_cell)
            logger.info(f"Updated Sheet Row {row_index} Status to '{status}'")
            
        except Exception as e:
            logger.error(f"Failed to update task status in sheet: {e}")

# IMPORTANT: You will need to install gspread and oauth2client
# pip install gspread oauth2client
