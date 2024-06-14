#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import psutil
import subprocess
import json
from collections import deque
from threading import Lock
from fcntl import flock, LOCK_EX, LOCK_UN
import curses
import time
import signal

# Determine the directory of this script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JOB_FILE = os.path.join(BASE_DIR, "jobs.json")
LOCK_FILE = os.path.join(BASE_DIR, "jobs.lock")

job_lock = Lock()

class Job:
    def __init__(self, job_id, user, command, priority, memory, compute):
        self.job_id = job_id
        self.user = user
        self.command = command
        self.priority = priority
        self.memory = memory
        self.compute = compute
        self.status = 'queued'
        self.pid = None

    def to_dict(self):
        return self.__dict__

    @staticmethod
    def from_dict(data):
        job = Job(data['job_id'], data['user'], data['command'], data['priority'], data['memory'], data['compute'])
        job.status = data['status']
        job.pid = data.get('pid', None)
        return job

def load_jobs():
    if not os.path.exists(JOB_FILE):
        return deque(), []
    with open(JOB_FILE, 'r') as f:
        data = json.load(f)
        job_queue = deque(Job.from_dict(job) for job in data['job_queue'])
        job_history = [Job.from_dict(job) for job in data['job_history']]
        return job_queue, job_history

def save_jobs(job_queue, job_history):
    with open(JOB_FILE, 'w') as f:
        data = {
            'job_queue': [job.to_dict() for job in job_queue],
            'job_history': [job.to_dict() for job in job_history],
        }
        json.dump(data, f, indent=4)

def submit_job(user, command, priority, memory, compute):
    with job_lock, open(LOCK_FILE, 'w') as lockfile:
        flock(lockfile, LOCK_EX)
        job_queue, job_history = load_jobs()
        job_id = len(job_history) + 1
        job = Job(job_id, user, command, priority, memory, compute)
        job_queue.append(job)
        job_history.append(job)
        save_jobs(job_queue, job_history)
        flock(lockfile, LOCK_UN)
        print(f"Job {job_id} submitted by {user}")

def run_next_job():
    with job_lock, open(LOCK_FILE, 'w') as lockfile:
        flock(lockfile, LOCK_EX)
        job_queue, job_history = load_jobs()
        if job_queue:
            job = job_queue.popleft()
            if psutil.virtual_memory().available >= job.memory:
                job.status = 'running'
                process = subprocess.Popen(job.command, shell=True)
                job.pid = process.pid
                job_history = [j for j in job_history if j.job_id != job.job_id]  # Remove old job status
                job_history.append(job)  # Add updated job status
                save_jobs(job_queue, job_history)  # Save the running status and PID
                flock(lockfile, LOCK_UN)  # Release the lock while the job is running

                # Check periodically if the job is still running
                while process.poll() is None:
                    with open(LOCK_FILE, 'w') as lockfile_inner:
                        flock(lockfile_inner, LOCK_EX)
                        job_queue, job_history = load_jobs()
                        job_history = [j for j in job_history if j.job_id != job.job_id]  # Remove old job status
                        job_history.append(job)  # Ensure job is still in job_history
                        save_jobs(job_queue, job_history)  # Save intermediate state if needed
                        flock(lockfile_inner, LOCK_UN)

                # Final status update after job completion
                with open(LOCK_FILE, 'w') as lockfile_final:
                    flock(lockfile_final, LOCK_EX)
                    job_queue, job_history = load_jobs()
                    if process.returncode == 0:
                        job.status = 'executed'
                    else:
                        job.status = 'failed'
                    job.pid = None
                    job_history = [j for j in job_history if j.job_id != job.job_id]  # Remove old job status
                    job_history.append(job)  # Add updated job status
                    save_jobs(job_queue, job_history)  # Save the final status
                    flock(lockfile_final, LOCK_UN)

            else:
                job_queue.appendleft(job)
                save_jobs(job_queue, job_history)
                print(f"Job {job.job_id} is on hold due to insufficient memory")
        else:
            flock(lockfile, LOCK_UN)  # Ensure the lock is released if no job is run

def update_job_status():
    with job_lock, open(LOCK_FILE, 'w') as lockfile:
        flock(lockfile, LOCK_EX)
        job_queue, job_history = load_jobs()
        updated_job_history = []
        for job in job_history:
            if job.status == 'running' and job.pid is not None:
                try:
                    proc = psutil.Process(job.pid)
                    if proc.status() in (psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD):
                        job.status = 'executed'
                        job.pid = None
                    elif not proc.is_running():
                        job.status = 'failed'
                        job.pid = None
                except psutil.NoSuchProcess:
                    job.status = 'failed'
                    job.pid = None
            if job.status != 'executed':
                updated_job_history.append(job)
        save_jobs(job_queue, updated_job_history)
        flock(lockfile, LOCK_UN)


def draw_progress_bar(stdscr, percentage, width=20):
    bar_width = int(percentage * width)
    bar = "[" + "#" * bar_width + "-" * (width - bar_width) + "]"
    return bar

def get_job_resource_usage(job):
    if job.pid:
        try:
            proc = psutil.Process(job.pid)
            memory_usage = proc.memory_info().rss / (1024 * 1024)  # Convert to MB
            cpu_usage = proc.cpu_percent(interval=0.1) / psutil.cpu_count()
            return memory_usage, cpu_usage
        except psutil.NoSuchProcess:
            return 0, 0
    return 0, 0

def display_jobs(stdscr):
    while True:
        with job_lock, open(LOCK_FILE, 'r') as lockfile:
            flock(lockfile, LOCK_EX)
            job_queue, job_history = load_jobs()
            flock(lockfile, LOCK_UN)

        stdscr.clear()
        stdscr.addstr(0, 0, f"{'JobID':<6} {'User':<10} {'Memory (MB)':<12} {'CPU (%)':<8} {'Status':<10} {'Command'}")

        row = 1
        for job in job_history:
            if job.status == 'running':
                memory_usage, cpu_usage = get_job_resource_usage(job)
                memory_bar = draw_progress_bar(stdscr, min(memory_usage / job.memory, 1.0))
                cpu_bar = draw_progress_bar(stdscr, min(cpu_usage / job.compute, 1.0))
                stdscr.addstr(row, 0, f"{job.job_id:<6} {job.user:<10} {memory_usage:<12.2f} {cpu_usage:<8.2f} {job.status:<10} {memory_bar} {cpu_bar} {job.command}")
            else:
                stdscr.addstr(row, 0, f"{job.job_id:<6} {job.user:<10} {'-':<12} {'-':<8} {job.status:<10} {job.command}")
            row += 1

        stdscr.refresh()
        time.sleep(1)

def list_jobs():
    curses.wrapper(display_jobs)

# Signal handler to gracefully exit on Ctrl+C
def signal_handler(sig, frame):
    sys.exit(0)

def prioritize_job(job_id):
    with job_lock, open(LOCK_FILE, 'w') as lockfile:
        flock(lockfile, LOCK_EX)
        job_queue, job_history = load_jobs()
        for job in job_queue:
            if job.job_id == job_id:
                job_queue.remove(job)
                job_queue.appendleft(job)
                save_jobs(job_queue, job_history)
                print(f"Job {job_id} has been prioritized")
                break
        flock(lockfile, LOCK_UN)

def clear_jobs_by_status(status):
    with job_lock, open(LOCK_FILE, 'w') as lockfile:
        flock(lockfile, LOCK_EX)
        job_queue, job_history = load_jobs()

        # Filter out jobs from job_queue and job_history based on status
        job_queue = deque([job for job in job_queue if job.status != status])
        job_history = [job for job in job_history if job.status != status]

        save_jobs(job_queue, job_history)
        flock(lockfile, LOCK_UN)
        print(f"Cleared jobs with status '{status}'")

signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: job_scheduler.py <command> [<args>]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "submit":
        user = sys.argv[2]
        cmd = sys.argv[3]
        priority = int(sys.argv[4])
        memory = int(sys.argv[5])
        compute = int(sys.argv[6])
        submit_job(user, cmd, priority, memory, compute)
    elif command == "run":
        run_next_job()
    elif command == "list":
        list_jobs()
    elif command == "prioritize":
        job_id = int(sys.argv[2])
        prioritize_job(job_id)
    elif command == "update":
        update_job_status()
    elif command == "clear":
        if len(sys.argv) == 4 and sys.argv[2] == "-s":
            status = sys.argv[3]
            clear_jobs_by_status(status)
        else:
            print("Usage: job_scheduler.py clear -s <status>")
    else:
        print("Unknown command")