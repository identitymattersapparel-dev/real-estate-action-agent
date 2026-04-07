import pandas as pd
import re
import os
from supabase import create_client
from datetime import datetime, timedelta

# Initialize Supabase from Environment Variables (Safe for work laptops)
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

def download_from_cloud(firm_name):
    """Downloads the CSV from Supabase Storage to Render's temporary memory."""
    file_name = f"{firm_name.replace(' ', '_')}_leads.csv"
    try:
        with open(file_name, 'wb+') as f:
            # Reaches into the private 'client-uploads' bucket
            res = supabase.storage.from_('client-uploads').download(file_name)
            f.write(res)
        return file_name
    except Exception as e:
        print(f"Cloud Storage Error for {firm_name}: {e}")
        return None

def clean_context(text):
    """Strips HTML to find 'Website Identification' and other gold."""
    if pd.isna(text) or not isinstance(text, str): return ""
    return re.sub(r'<.*?>', ' ', text).strip()

def score_lead(row):
    """Applies the 8-Segment Weighted Logic."""
    score = 0
    notes = str(row.get('clean_context', '')).lower()
    # Segment 7: Recent Activity
    if "website identification" in notes or "fello click" in notes:
        score += 15
    # Segment 1: Long-term ownership (Proxy check)
    if "15 year" in str(row.get('Tags', '')).lower():
        score += 10
    # Segment 5: Warm/Hot
    if "hot" in str(row.get('Stage', '')).lower():
        score += 8
    return score

def start_audit_cycle():
    # 1. Check who is due in the 'client_jobs' table
    jobs = supabase.table("client_jobs").select("*, clients(*)").lte("next_run_due", datetime.now().isoformat()).execute()
    
    if not jobs.data:
        print("No audits due today. System idling.")
        return

    for job in jobs.data:
        firm = job['clients']
        print(f"Beginning Audit for {firm['firm_name']}...")
        
        # 2. Grab the file from the cloud
        file_path = download_from_cloud(firm['firm_name'])
        
        if file_path:
            df = pd.read_csv(file_path, low_memory=False)
            df['clean_context'] = df['Notes'].apply(clean_context) # Adjust 'Notes' to your CRM col name
            df['score'] = df.apply(score_lead, axis=1)
            
            # 3. Filter top leads (respecting the daily_lead_limit)
            top_leads = df.sort_values(by='score', ascending=False).head(firm['daily_lead_limit'])
            
            for _, lead in top_leads.iterrows():
                # Notify & Log
                print(f"Lead Found: {lead['Name']} (Score: {lead['score']})")
                # (Insert your SMS/Email notification trigger here)

            # 4. Cleanup & Re-schedule
            os.remove(file_path) # Delete temp file from Render
            next_run = (datetime.now() + timedelta(days=30)).isoformat()
            supabase.table("client_jobs").update({"next_run_due": next_run}).eq("id", job['id']).execute()

if __name__ == "__main__":
    start_audit_cycle()
