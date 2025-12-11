import time
import logging

import schedule

from agents.orchestrator import run_full_pipeline_for_authority

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def job_for_authority(authority_code: str):
    logging.info("Scheduled job started for %s.", authority_code)
    run_full_pipeline_for_authority(authority_code)
    logging.info("Scheduled job finished for %s.", authority_code)


if __name__ == "__main__":
    # BCL toutes les 6 heures, ECB toutes les 12 heures (exemple)
    schedule.every(6).hours.do(job_for_authority, authority_code="BCL")
    schedule.every(12).hours.do(job_for_authority, authority_code="ECB")

    logging.info("Scheduler started. Press Ctrl+C to stop.")

    while True:
        schedule.run_pending()
        time.sleep(60)
