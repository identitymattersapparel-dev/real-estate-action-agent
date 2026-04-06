import pandas as pd
import os
import google.generativeai as genai
from supabase import create_client

# 1. Setup Connections (Render Environment Variables)
genai.configure(api_key=os.environ["GEMINI_API_KEY"])

# Professional Guardrails for Tier 1 2026 Stable Model
model = genai.GenerativeModel(
    model_name='gemini-2.5-flash',
    system_instruction="You are a professional real estate data analyst. Use clinical, professional language. Do not use informal or personified terms like 'hot' to describe people. Instead, use 'High Priority' or 'Immediate Follow-up'."
)

supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

def process_daily_leads(csv_file):
    # Load the data
    df = pd.read_csv(csv_file)
    
    # Filter for High Priority leads and sort by most recent
    df['Last Contact Date'] = pd.to_datetime(df['Last Contact Date'])
    high_priority = df[df['Priority Score'] == 'High'].sort_values(by='Last Contact Date', ascending=False)

    # Pick the top 10 leads to evaluate
    top_leads = high_priority.head(10)
    
    for _, lead in top_leads.iterrows():
        # Check Supabase: Skip if this specific email was already handled today
        res = supabase.table("processed_leads").select("*").eq("lead_email", lead['Email']).execute()
        if len(res.data) > 0:
            print(f"Skipping {lead['Name']} - already processed.")
            continue  

        # Professional Data-Driven Prompt
        prompt = f"""
        Analyze the following real estate lead and provide a data-driven 3-step action plan.
        
        Name: {lead['Name']}
        Status: {lead['Reason']}
        Last Contact: {lead['Last Contact Date']}
        Suggested Opening: {lead['Suggested Opening']}
        
        Output a concise 'One Big Thing' action plan for an agent.
        """
        
        # This will now loop through all 10 because the 'break' is gone
        response = model.generate_content(prompt)
        
        print(f"--- ACTION PLAN FOR {lead['Name']} ---")
        print(response.text)
        
        # Save to Supabase Memory so we don't repeat this lead tomorrow
        supabase.table("processed_leads").insert({
            "lead_email": lead['Email'],
            "summary": response.text
        }).execute()

if __name__ == "__main__":
    process_daily_leads('Scout_Master_Leads (1).csv')
