import unittest
from phoenix.workflow import *


class TestWorkflow(Workflow):
    def __init__(self):
        self.steps = 0
        self.stopped = False

    def run_step(self):
        self.steps += 1
        return 0.1 if self.steps < 10 else END_WORKFLOW

    def on_early_stop(self):
        self.stopped = True


class TestFunctions(unittest.TestCase):
    def test_simple_behavior(self):
        engine = WorkflowEngine(worker_count=5)

        workflows = []

        for i in range(10):
            workflows.append(TestWorkflow())
            engine.add(workflows[i], delay_secs=0.1 * i)

        sleep(2)
        engine.stop()
        sleep(1)

        for i in range(10):
            self.assertEqual(10, workflows[i].steps)
            self.assertFalse(workflows[i].stopped)

    def test_early_stop(self):
        engine = WorkflowEngine(worker_count=5)

        workflows = []

        for i in range(10):
            workflows.append(TestWorkflow())
            engine.add(workflows[i], delay_secs=0.1 * i)

        sleep(0.5)
        engine.stop()
        sleep(1)

        for i in range(10):
            if i < 5:
                self.assertTrue(1 <= workflows[i].steps <= 10)
            else:
                self.assertFalse(1 < workflows[i].steps)
            self.assertTrue(workflows[i].stopped)
