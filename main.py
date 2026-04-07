import pandas as pd
import re
import os
import io
from supabase import create_client
from datetime import datetime, timedelta

# Initialize from Render Env Vars
supabase = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

def get_latest_file_from_storage(firm_name):
    """Finds the most recent file in the bucket for this firm without manual naming."""
    prefix = firm_name.replace(' ', '_')
    files = supabase.storage.from_('client-uploads').list()
    
    # Filter files that match the firm name (case-insensitive)
    matches = [f for f in files if f['name'].lower().startswith(prefix.lower())]
    if not matches:
        return None, None
    
    # Sort by created_at to get the newest upload
    latest_file = sorted(matches, key=lambda x: x['created_at'], reverse=True)[0]['name']
    
    response = supabase.storage.from_('client-uploads').download(latest_file)
    return io.BytesIO(response), latest_file

def auto_discover_columns(df):
    """Identifies the 'Intent Column' (Notes/Messages) without hard-coded lists."""
    # Look for the column with the highest average string length (where the HTML/Notes live)
    potential_cols = df.select_dtypes(include=['object']).columns
    best_col = None
    max_len = 0
    
    for col in potential_cols:
        avg_len = df[col].str.len().mean()
        if avg_len > max_len:
            max_len = avg_len
            best_col = col
    return best_col

def run_zero_touch_audit():
    # 1. Who is due?
    jobs = supabase.table("client_jobs").select("*, clients(*)").lte("next_run_due", datetime.now().isoformat()).execute()
    
    for job in jobs.data:
        client = job['clients']
        firm_id = client['id']
        
        # 2. Automated File Discovery
        file_io, actual_filename = get_latest_file_from_storage(client['firm_name'])
        if not file_io:
            print(f"Skipping {client['firm_name']}: No file found in storage.")
            continue

        df = pd.read_csv(file_io, low_memory=False)

        # 3. Automated Column Discovery
        note_col = auto_discover_columns(df)
        email_col = next((c for c in df.columns if 'email' in c.lower()), 'Email 1')

        # 4. Scoring Logic (Applied to the discovered note column)
        def score_row(row):
            score = 0
            val = str(row.get(note_col, '')).lower()
            if "website identification" in val or "fello click" in val: score += 15
            if "15 year" in str(row).lower(): score += 10 # Global search in row
            return score

        df['score'] = df.apply(score_row, axis=1)

        # 5. Deduplicate against Supabase
        processed = supabase.table("processed_leads").select("lead_email").eq("client_id", firm_id).execute()
        seen = {p['lead_email'] for p in processed.data}
        
        # 6. Process Top Leads
        top_leads = df[~df[email_col].isin(seen)].sort_values(by='score', ascending=False).head(client['daily_lead_limit'])

        for _, lead in top_leads.iterrows():
            supabase.table("processed_leads").insert({
                "client_id": firm_id,
                "lead_email": lead[email_col],
                "agent_assigned": lead.get('Assigned To', 'Unassigned')
            }).execute()
            print(f"[{client['firm_name']}] Surfaced: {lead.get('Name')} | Score: {lead['score']}")

        # 7. Auto-Schedule Next Run (30 days)
        next_run = (datetime.now() + timedelta(days=30)).isoformat()
        supabase.table("client_jobs").update({"next_run_due": next_run, "last_run_date": datetime.now().isoformat()}).eq("id", job['id']).execute()

if __name__ == "__main__":
    run_zero_touch_audit()
