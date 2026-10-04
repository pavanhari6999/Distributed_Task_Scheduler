from datetime import datetime


class Task:
    def __init__(self, task_id, name, priority="Medium"):
        self.task_id = task_id
        self.name = name
        self.priority = priority
        self.status = "Waiting"
        self.worker = "-"
        self.progress = 0
        self.created_at = datetime.now().strftime("%H:%M:%S")
        self.started_at = "-"
        self.completed_at = "-"

    def start(self, worker):
        self.status = "Running"
        self.worker = worker
        self.progress = 0
        self.started_at = datetime.now().strftime("%H:%M:%S")

    def update_progress(self, progress):
        self.progress = progress

    def complete(self):
        self.status = "Completed"
        self.progress = 100
        self.completed_at = datetime.now().strftime("%H:%M:%S")

    def to_dict(self):
        return {
            "id": self.task_id,
            "name": self.name,
            "priority": self.priority,
            "status": self.status,
            "worker": self.worker,
            "progress": self.progress,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at
        }