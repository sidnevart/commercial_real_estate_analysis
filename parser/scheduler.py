#!/usr/bin/env python3
"""
Scheduler for running the parser at specific times.
Run this script once to set up the scheduler.
"""

import logging
import os
import sys
from datetime import datetime
from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("scheduler.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)

def run_parser():
    """Execute the main parser script."""
    log.info("Starting scheduled parsing job")
    try:
        project_dir = os.path.dirname(os.path.abspath(__file__))
        
        os.chdir(project_dir)
        
        os.system(f"{sys.executable} -m parser.main")
        
        log.info("Parsing job completed successfully")
    except Exception as e:
        log.error(f"Error running parsing job: {e}", exc_info=True)

def main():
    """Set up and start the scheduler."""
    scheduler = BlockingScheduler()
    
    scheduler.add_job(
        run_parser,
        trigger=CronTrigger(hour=3, minute=0),
        id='parsing_job',
        name='Run parsing at 3 AM daily',
        replace_existing=True
    )
    
    scheduler.add_job(
        run_parser,
        trigger='date',
        run_date=datetime.now(),
        id='initial_run',
        name='Initial parser run'
    )
    
    log.info("Scheduler started. Parser will run at 3:00 AM daily.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler stopped")

if __name__ == "__main__":
    main()