import pandas as pd
import os
import google.generativeai as genai
from supabase import create_client

# 1. Setup Connections (Render Environment Variables)
genai.configure(api_key=os.environ["GEMINI_API_KEY"])
model = genai.GenerativeModel('gemini-2.0-flash')
supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

def process_daily_leads(csv_file):
    # Load the data
    df = pd.read_csv(csv_file)
    
    # Filter for High Priority leads that were active recently
    # We convert 'Last Contact Date' to a real date format
    df['Last Contact Date'] = pd.to_datetime(df['Last Contact Date'])
    high_priority = df[df['Priority Score'] == 'High'].sort_values(by='Last Contact Date', ascending=False)

    # Pick the top 10 leads to evaluate
    top_leads = high_priority.head(10)
    
    for _, lead in top_leads.iterrows():
        # Check Supabase: Have we already processed this email today?
        res = supabase.table("processed_leads").select("*").eq("lead_email", lead['Email']).execute()
        if len(res.data) > 0:
            continue  # Skip if already handled

        # The Prompt for the "One Big Thing"
        prompt = f"""
        Act as an elite Real Estate Coach. Review this lead:
        Name: {lead['Name']}
        Status: {lead['Reason']}
        Last Contact: {lead['Last Contact Date']}
        Suggested Opening: {lead['Suggested Opening']}
        
        Write a concise 'One Big Thing' action plan (3 steps) for an agent.
        """
        
        response = model.generate_content(prompt)
        
        # Output the plan (you can connect this to Telegram/Email)
        print(f"--- ACTION PLAN FOR {lead['Name']} ---")
        print(response.text)
        
        # Save to Supabase Memory
        supabase.table("processed_leads").insert({
            "lead_email": lead['Email'],
            "summary": response.text
        }).execute()
        
        break # We only want ONE big thing per day

if __name__ == "__main__":
    process_daily_leads('Scout_Master_Leads (1).csv')
