from datetime import datetime, timedelta
import uuid


class Job(object):

    def __init__(self, func, trigger, **config):
        job_id = str(uuid.uuid4()).split("-")
        job_id = job_id[len(job_id)-1]

        self.id = job_id
        self.function = func
        self.trigger = trigger
        self.config = config


class ContextScheduler(object):
    contexts = {}

    def __init__(self, scheduler, **config):
        context_id = str(uuid.uuid4()).split("-")
        context_id = context_id[len(context_id)-1]

        self.id = context_id
        ContextScheduler.contexts[self.id] = self

        self.jobs = []
        self.scheduler = scheduler
        self.data = config['data']
        self.deadline = config['deadline']
        self.on_start = config['on_start']
        self.on_end = config['on_end']

    def attach_job(self, job):
        self.jobs.append(job)

    def start(self):
        if self.deadline is not None:
            now = datetime.now()
            deadline = now + timedelta(**self.deadline)
            self.deadline_date = deadline

            end_job = Job(
                self.on_end,
                'date',
                run_date=deadline
            )

            self.jobs.append(end_job)

        self.on_start(self)

        for job in self.jobs:
            if job.trigger == 'interval' and self.deadline is not None:
                job.config['end_date'] = deadline

            self.scheduler.add_job(
                job.function,
                job.trigger,
                [self],
                id=job.id,
                **job.config
            )

    def stop(self):
        for job in self.jobs:
            self.scheduler.pause_job(job_id=job.id)
            self.scheduler.remove_job(job_id=job.id)

        self.on_end(self)

    def clear(self):
        if self.id in ContextScheduler.contexts:
            del ContextScheduler.contexts[self.id]

    @staticmethod
    def get_context(context_id):
        context = None

        if context_id in ContextScheduler.contexts:
            context = ContextScheduler.contexts[context_id]

        return context
