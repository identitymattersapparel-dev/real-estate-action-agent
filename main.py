This is the "Universal V2.1" script. It includes the Auto-Mapper for dynamic column detection, the HTML Scrubber for extracting "gold" from CRM notes, and the Supabase Cloud integration to ensure your work laptop stays completely out of the loop.

The Universal SaaS Engine (main.py)
Python
import pandas as pd
import re
import os
import io
from supabase import create_client
from datetime import datetime, timedelta

# 1. SETUP: Initializing from Render Environment Variables
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# 2. THE UTILITIES: Cleaning & Fetching
def clean_html_notes(text):
    """Strips HTML tags to find 'Website Identification' and other high-intent signals."""
    if pd.isna(text) or not isinstance(text, str):
        return ""
    # Strip HTML and collapse extra whitespace
    clean = re.sub(r'<.*?>', ' ', text)
    return re.sub(r'\s+', ' ', clean).strip()

def download_csv_from_cloud(firm_name):
    """Downloads the CSV from Supabase Storage 'client-uploads' bucket."""
    file_name = f"{firm_name.replace(' ', '_')}_leads.csv"
    try:
        # Download as bytes to avoid local file writing where possible
        response = supabase.storage.from_('client-uploads').download(file_name)
        return io.BytesIO(response), file_name
    except Exception as e:
        print(f"Error downloading {file_name}: {e}")
        return None, None

# 3. THE BRAIN: Scoring & Segmenting
def calculate_lead_score(row, note_col):
    """Ranks leads based on the 8-Segment Strategy using available data."""
    score = 0
    # Extract context from the dynamically mapped notes column
    notes = str(row.get(note_col, '')).lower() if note_col else ""
    tags = str(row.get('Tags', '')).lower()
    stage = str(row.get('Stage', '')).lower()

    # Segment 7: Recently Active (Top Priority)
    if "website identification" in notes or "fello click" in notes:
        score += 15
    
    # Segments 1-4: High Equity / Long-term Ownership
    if "15 year" in tags or "10 year" in tags:
        score += 10
        
    # Segment 5: Warm/Hot Stages
    if any(word in stage for word in ['hot', 'qualified', 'active', 'pao']):
        score += 8

    # Basic Contactability
    if pd.notna(row.get('Email 1')) or pd.notna(row.get('Phone 1')):
        score += 2
        
    return score

# 4. THE ENGINE: The Master Loop
def run_universal_audit():
    # A. Check Supabase for clients due for an audit
    # Uses 'client_jobs' to see who is scheduled for today or earlier
    jobs = supabase.table("client_jobs").select("*, clients(*)").lte("next_run_due", datetime.now().isoformat()).execute()
    
    if not jobs.data:
        print(f"[{datetime.now()}] No audits due. System idling.")
        return

    for job in jobs.data:
        client = job['clients']
        firm_name = client['firm_name']
        limit = client['daily_lead_limit']
        
        print(f"--- Starting Audit: {firm_name} ---")
        
        # B. Fetch the file from Supabase Storage
        file_io, file_name = download_csv_from_cloud(firm_name)
        if not file_io:
            continue

        # C. Load Data
        df = pd.read_csv(file_io, low_memory=False)

        # D. DYNAMIC COLUMN MAPPING (The 'Auto-Mapper')
        # We look for the most likely 'Gold Mine' column
        potential_notes = ['Notes', 'Message', 'Description', 'Comments', 'Activity Log', 'Last Note']
        note_col = next((col for col in potential_notes if col in df.columns), None)
        
        if note_col:
            print(f"Mapped 'Gold Mine' to column: {note_col}")
            # Scrub the HTML for better AI context later
            df['clean_context'] = df[note_col].apply(clean_html_notes)
        else:
            print("Warning: No Notes/Message column detected. Scoring on Tags/Stage only.")
            df['clean_context'] = ""

        # E. Score and Filter
        df['internal_score'] = df.apply(lambda row: calculate_lead_score(row, 'clean_context'), axis=1)
        
        # Deduplicate: Don't re-process emails already in 'processed_leads' for this client
        processed = supabase.table("processed_leads").select("lead_email").eq("client_id", client['id']).execute()
        past_emails = [p['lead_email'] for p in processed.data]
        df = df[~df['Email 1'].isin(past_emails)]

        # F. Select Top Leads
        top_leads = df.sort_values(by='internal_score', ascending=False).head(limit)

        # G. Logging & Completion
        for _, lead in top_leads.iterrows():
            # Log to Supabase so we don't repeat this lead next month
            supabase.table("processed_leads").insert({
                "client_id": client['id'],
                "lead_email": lead['Email 1'],
                "agent_assigned": lead.get('Assigned To', 'Unassigned')
            }).execute()
            
            print(f"SURFACED: {lead['Name']} | Score: {lead['internal_score']} | Agent: {lead.get('Assigned To')}")

        # H. Update the Job Tracker (Push next run out by 30 days)
        next_run = (datetime.now() + timedelta(days=30)).isoformat()
        supabase.table("client_jobs").update({
            "last_run_date": datetime.now().isoformat(),
            "next_run_due": next_run,
            "status": "completed"
        }).eq("id", job['id']).execute()
        
        print(f"Audit for {firm_name} complete. Next run scheduled for {next_run}.")

if __name__ == "__main__":
    run_universal_audit()
