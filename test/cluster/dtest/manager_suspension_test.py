import logging
import time

import pytest

from dtest_class import Tester
from dtest_scylla_manager import (
    ScyllaManagerError,
    ScyllaManagerMixin,
    TaskStatus,
    create_cron_list_from_timedelta,
)
from tools.cluster_topology import generate_cluster_topology

CLUSTER_NAME = "cluster1"
logger = logging.getLogger(__name__)


@pytest.mark.scylla_manager
class TestScyllaManagerSuspension(Tester, ScyllaManagerMixin):
    def test_create_task_while_suspended(self):
        topology_layout = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=2)
        self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(self.cluster.nodelist()[0], name=CLUSTER_NAME)

        mgr_cluster.suspend()
        try:
            mgr_cluster.repair_api.repair(cluster_name=mgr_cluster.id)
        except ScyllaManagerError as err:
            assert "suspended" in err.args[0].lower() and "scheduling tasks is not allowed" in err.args[0].lower(), f"Task creation failed, as expected, but not with proper error message: {err.args[0]}"
        else:
            raise AssertionError("Test creation while the manager is suspended did not fail")

    def test_suspend_on_resume_start_tasks_without_duration(self):
        """
        New in manager 3.0

        The on-resume-start-tasks flag in the sctool suspend command is meant to be used in conjunction
        with the duration flag.
        If on-resume-start-tasks is used without duration, the command should fail. The test makes sure
        of that.
        """
        topology_layout = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=2)
        self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(self.cluster.nodelist()[0], name=CLUSTER_NAME)

        try:
            mgr_cluster.suspend(on_resume_start_tasks=True)
        except ScyllaManagerError as err:
            assert "duration" in err.args[0].lower(), f"Suspending the cluster with the 'on-resume-start-tasks' flag but without  duration failed, but without a proper error message: {err.args[0]}"
        else:
            raise ScyllaManagerError("Suspending the cluster with 'on-resume-start-tasks' but without 'duration' did not fail")

    def test_create_scheduled_task_while_suspended(self):
        """
        When suspended, the manager does not allow the user scheduling tasks in the future,
        not even more than 8 hours in the future (as in prior to version 3.0).
        This test tries to schedule a task in the future while the manager is suspended, expecting failure
        """
        topology_layout = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=2)
        self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(self.cluster.nodelist()[0], name=CLUSTER_NAME)

        mgr_cluster.suspend()
        try:
            intended_run_time_cron = create_cron_list_from_timedelta(minutes=2, hours=8)
            mgr_cluster.repair_api.repair(cluster_name=mgr_cluster.id, cron=intended_run_time_cron)
        except ScyllaManagerError as err:
            assert "suspended" in err.args[0].lower() and "scheduling tasks is not allowed" in err.args[0].lower(), (
                f"Scheduling a task in the future while the manager is suspended failed, as expected, but not with proper error message: {err.args[0]}"
            )
        else:
            raise AssertionError("Task scheduling withing the next 8 hours while the manager is suspended did not fail")

    def test_schedule_task_to_run_while_suspended(self):
        topology_layout = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=3)
        self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(self.cluster.nodelist()[0], name=CLUSTER_NAME)
        intended_run_time_cron = create_cron_list_from_timedelta(minutes=2)
        repair_task = mgr_cluster.repair_api.repair(cluster_name=mgr_cluster.id, cron=intended_run_time_cron)

        mgr_cluster.suspend()

        # Wait 2 minutes plus to make sure that is started unless suspended
        time.sleep(200)
        repair_task_status = repair_task.status
        assert repair_task_status == TaskStatus.NEW, f'Task that was set to run while the manager was suspended has a Next Run value of "{repair_task_status}" instead of the expected `NEW`'

        mgr_cluster.resume()

        assert not len(repair_task.history), f"The task has ran while the manager was suspended"
        repair_task_status = repair_task.status
        assert repair_task_status == TaskStatus.NEW, f"The task was expected to reach NEW status, instead it reached {repair_task_status!s}"
