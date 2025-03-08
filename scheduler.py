import schedule
import time
import subprocess

def job():
    print("Running main.py...")
    subprocess.run(["python3", "main.py"]) 

schedule.every(7).days.do(job)

print("Scheduler started. The script will run every 7 days.")

while True:
    schedule.run_pending()
    time.sleep(60) 
