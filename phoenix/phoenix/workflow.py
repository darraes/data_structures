from abc import ABC, abstractmethod
from time import perf_counter, sleep
from threading import RLock, Thread, Condition
from phoenix.monitoring import log
from heapq import heappop, heappush

MS_IN_SEC = 0.001
END_WORKFLOW = -1
WAIT_FOREVER = float("inf")


class Workflow(ABC):
    @abstractmethod
    def run_step(self):
        """
        Executes the next step and returns the minimal wait time, in fractional seconds,
        the workflow engine should wait before calling back into the |run_step| method.
        """
        return END_WORKFLOW

    @abstractmethod
    def on_early_stop(self):
        """
        Called on the workflow objects when the engine is stopping before completion
        """
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
        self._signal = Condition(self._lock)
        self._stopping = False
        self._thread = Thread(target=self._run)
        #self._thread.daemon = True
        self._thread.start()

    def add_task(self, workflow, delay_secs=0.0):
        self._lock.acquire()
        self._idle_tasks.append(WorkflowTask(perf_counter() + delay_secs, workflow))
        self._signal.notify()
        self._lock.release()

    def stop(self):
        self._lock.acquire()
        log.info("Stopping workflows with %d tasks remaining",
                 len(self._idle_tasks) + len(self._running_tasks))
        self._stopping = True
        self._signal.notify()
        self._lock.release()

    def _run(self):
        while not self._stopping:
            for _, task in self._running_tasks:
                if task.when >= 0 and task.when <= perf_counter():
                    task.when = task.workflow.run_step()
                    if task.when != END_WORKFLOW:
                        task.when += perf_counter()

            while self._running_tasks and self._running_tasks[0][1].when == END_WORKFLOW:
                heappop(self._running_tasks)

            self._lock.acquire()
            for t in self._idle_tasks:
                heappush(self._running_tasks, (t.when, t))
            self._idle_tasks = []

            if not self._running_tasks:
                self._signal.wait()
            elif self._running_tasks[0][1].when > perf_counter():
                self._signal.wait(timeout=self._running_tasks[0][1].when - perf_counter())
                
            self._lock.release()

        for _, t in self._idle_tasks + self._running_tasks:
            t.workflow.on_early_stop()


class WorkflowEngine(object):
    def __init__(self, worker_count):
        self._lock = RLock()
        self._add_on = 0
        self._worker_count = worker_count
        self._workers = []
        for _ in range(worker_count):
            self._workers.append(WorkflowWorker())

    def add(self, workflow, delay_secs=0.0):
        with self._lock:
            self._workers[self._add_on].add_task(workflow, delay_secs)
            self._add_on = (self._add_on + 1) % self._worker_count

    def stop(self):
        log.info("Stopping all workflows workers (%d)", self._worker_count)
        with self._lock:
            for worker in self._workers:
                worker.stop()
