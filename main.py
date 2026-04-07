import pandas as pd
import re
import os
import io
from supabase import create_client
from datetime import datetime, timedelta

# --- 1. THE CONNECTION (Airgapped from Laptop) ---
supabase = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

def get_latest_client_file(firm_name):
    """Finds the newest file for this firm in Supabase Storage."""
    prefix = firm_name.replace(' ', '_')
    try:
        files = supabase.storage.from_('client-uploads').list()
        matches = [f for f in files if f['name'].lower().startswith(prefix.lower())]
        if not matches: return None
        # Sort by most recent upload
        latest = sorted(matches, key=lambda x: x['created_at'], reverse=True)[0]['name']
        res = supabase.storage.from_('client-uploads').download(latest)
        return io.BytesIO(res)
    except Exception as e:
        print(f"Storage Error for {firm_name}: {e}")
        return None

def auto_map_columns(df):
    """Detects 'Gold Mine' (Notes) and 'Email' columns without hard-coding."""
    cols = df.columns.tolist()
    # Find the column with the most text (the Notes/HTML blob)
    note_col = max(df.select_dtypes(include=['object']).columns, 
                   key=lambda c: df[c].str.len().mean(), default=None)
    # Find the email column (fuzzy match)
    email_col = next((c for c in cols if 'email' in c.lower()), 'Email 1')
    return note_col, email_col

def run_the_machine():
    # A. Who is due for an audit?
    jobs = supabase.table("client_jobs").select("*, clients(*)").lte("next_run_due", datetime.now().isoformat()).execute()
    
    if not jobs.data:
        print(f"[{datetime.now()}] No audits due. Standing by.")
        return

    for job in jobs.data:
        client = job['clients']
        firm_name = client['firm_name']
        
        print(f"--- Starting Audit for {firm_name} ---")
        
        # B. Get Data
        file_io = get_latest_client_file(firm_name)
        if not file_io: continue
        
        df = pd.read_csv(file_io, low_memory=False)
        note_col, email_col = auto_map_columns(df)

        # C. Score Leads (8-Segment Logic)
        def score_logic(row):
            score = 0
            context = str(row.get(note_col, '')).lower()
            if "website identification" in context or "fello click" in context: score += 15
            if "15 year" in str(row).lower(): score += 10 # Search entire row for tags
            return score

        df['scout_score'] = df.apply(score_logic, axis=1)

        # D. Memory Check (Don't repeat leads)
        processed = supabase.table("processed_leads").select("lead_email").eq("client_id", client['id']).execute()
        past_emails = {p['lead_email'] for p in processed.data}
        
        # E. Process & Log
        top_leads = df[~df[email_col].isin(past_emails)].sort_values(by='scout_score', ascending=False).head(client['daily_lead_limit'])

        for _, lead in top_leads.iterrows():
            supabase.table("processed_leads").insert({
                "client_id": client['id'],
                "lead_email": lead[email_col],
                "agent_assigned": lead.get('Assigned To', 'Unassigned')
            }).execute()
            print(f"Scouted: {lead.get('Name', 'Unknown')} (Score: {lead['scout_score']})")

        # F. DYNAMIC SCHEDULER (Hands-Off)
        days = client.get('frequency_days') or 30
        next_run = (datetime.now() + timedelta(days=days)).isoformat()
        
        supabase.table("client_jobs").update({
            "next_run_due": next_run,
            "last_run_date": datetime.now().isoformat(),
            "status": "completed"
        }).eq("id", job['id']).execute()
        
        print(f"Success. {firm_name} scheduled for {next_run}.")

if __name__ == "__main__":
    run_the_machine()
