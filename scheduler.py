import schedule
import time
import subprocess

def job():
    print("Running main.py...")
    subprocess.run(["python3", "main.py"]) 

schedule.every(7).days.do(job)

while True:
    schedule.run_pending()
    time.sleep(60) 
