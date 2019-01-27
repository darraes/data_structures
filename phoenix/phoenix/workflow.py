from abc import ABC
from time import clock, sleep
from threading import RLock, Thread
from monitoring import log

MS_IN_SEC = 0.001
END_WORKFLOW = -1


class Workflow(ABC):
    @abstractmethod
    def run_step(self):
        return END_WORKFLOW

    @abstractmethod
    def on_stopping(self):
        pass


class WorkflowTask(object):
    def __init__(self, when, workflow):
        self.when = when
        self.workflow = workflow


class WorkflowWorker(object):
    def __init__(self):
        self._running_tasks = []
        self._idle_tasks = []
        self._lock = RLock()
        self._stopping = False
        self._thread = Thread(target=self._run)
        self._thread.daemon = True
        self._thread.start()

    def add_task(self, workflow, delay_secs=0.0):
        with self._lock:
            self._idle_tasks.append(WorkflowTask(clock() + delay_secs, workflow))

    def stop(self):
        self._stopping = True

    def _run(self):
        while not self._stopping:
            worked = False
            for task in self._running_tasks:
                if task.when >= 0 and task.when <= clock():
                    worked = True
                    task.when = task.workflow.run_step()
                    if task.when != END_WORKFLOW:
                        task.when += clock()

            self._running_tasks = [
                t for t in self._running_tasks if t.when != END_WORKFLOW
            ]

            with self._lock:
                self._running_tasks.extend(self._idle_tasks)
                self._idle_tasks = []

            if not worked:
                sleep(MS_IN_SEC)

        log.info("Workflow worker stopped")
        for t in self._idle_tasks + self._running_tasks:
            t.workflow.on_stopping()


class WorkflowEngine(object):
    def __init__(self, worker_count):
        self._lock = RLock()
        self._add_on = 0
        self._worker_count = worker_count
        self._workers = []
        for i in range(worker_count):
            self._workers.append(WorkflowWorker())

    def add_workflow(self, workflow, delay_secs=0.0):
        with self._lock:
            self._workers[self._add_on].add_task(workflow, delay_secs)
            self._add_on = (self._add_on + 1) % self._worker_count

    def stop(self):
        log.info("Stopping workflows")
        with self._lock:
            for worker in self._workers:
                worker.stop()
