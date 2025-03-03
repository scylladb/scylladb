import os
import pytest
import tempfile
from multiprocessing import Process, Queue
from typing import List

from attr import attrs, attrib

from test.pylib.db.writer import SQLiteWriter, TESTS_TABLE

@attrs(auto_attribs=True)
class TestData:
    architecture: str
    directory: str
    mode: str
    run_id: int
    test_name: str

def write_test_data(db_path: str, data: List[TestData], queue: Queue) -> None:
    """Helper function to write data in a separate process"""
    writer = SQLiteWriter(db_path)
    try:
        for item in data:
            row_id = writer.write_row(item, TESTS_TABLE)
            queue.put(row_id)
    except Exception as e:
        queue.put(e)

@pytest.fixture
def db_path():
    """Fixture to create a temporary database file"""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_path = tmp.name
    yield tmp_path
    os.unlink(tmp_path)

@pytest.fixture
def writer(db_path):
    """Fixture to create a SQLiteWriter instance"""
    return SQLiteWriter(db_path)

def test_basic_write(writer):
    """Test basic write functionality"""
    test_data = TestData(
        architecture="x86_64",
        directory="/test/dir",
        mode="debug",
        run_id=1,
        test_name="test_basic"
    )
    row_id = writer.write_row(test_data, TESTS_TABLE)
    assert row_id > 0

def test_write_multiple_rows(writer):
    """Test writing multiple rows"""
    test_data = [
        TestData(architecture="x86_64", directory="/test/dir1", mode="debug", run_id=1, test_name="test1"),
        TestData(architecture="arm64", directory="/test/dir2", mode="release", run_id=2, test_name="test2")
    ]
    writer.write_multiple_rows(test_data, TESTS_TABLE)
    
    # Verify data was written
    with writer.get_cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {TESTS_TABLE}")
        count = cursor.fetchone()[0]
        assert count == 2

def test_write_if_not_exist(writer):
    """Test write_row_if_not_exist functionality"""
    test_data = TestData(
        architecture="x86_64",
        directory="/test/dir",
        mode="debug",
        run_id=1,
        test_name="test_duplicate"
    )
    
    # First write
    first_id = writer.write_row_if_not_exist(test_data, TESTS_TABLE)
    # Second write of the same data
    second_id = writer.write_row_if_not_exist(test_data, TESTS_TABLE)
    
    assert first_id == second_id
    
    with writer.get_cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {TESTS_TABLE}")
        count = cursor.fetchone()[0]
        assert count == 1

def test_multiprocess_write(db_path):
    """Test writing from multiple processes"""
    processes = []
    queues = []
    num_processes = 4
    rows_per_process = 5
    
    # Create test data for each process
    for i in range(num_processes):
        queue = Queue()
        test_data = [
            TestData(
                architecture=f"arch{i}",
                directory=f"/test/dir{i}_{j}",
                mode="debug",
                run_id=i,
                test_name=f"test{i}_{j}"
            )
            for j in range(rows_per_process)
        ]
        
        process = Process(
            target=write_test_data,
            args=(db_path, test_data, queue)
        )
        processes.append(process)
        queues.append(queue)
    
    # Start all processes
    for p in processes:
        p.start()
    
    # Wait for all processes to complete
    for p in processes:
        p.join()
    
    # Check results
    results = []
    for q in queues:
        while not q.empty():
            result = q.get()
            if isinstance(result, Exception):
                raise result
            results.append(result)
    
    # Verify all writes were successful
    assert len(results) == num_processes * rows_per_process
    
    # Verify data in database
    writer = SQLiteWriter(db_path)
    with writer.get_cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {TESTS_TABLE}")
        count = cursor.fetchone()[0]
        assert count == num_processes * rows_per_process

def test_concurrent_write_same_data(db_path):
    """Test concurrent writes of the same data"""
    processes = []
    queues = []
    num_processes = 4
    
    # Create same test data for all processes
    test_data = [TestData(
        architecture="x86_64",
        directory="/test/dir",
        mode="debug",
        run_id=1,
        test_name="test_concurrent"
    )]
    
    for _ in range(num_processes):
        queue = Queue()
        process = Process(
            target=write_test_data,
            args=(db_path, test_data, queue)
        )
        processes.append(process)
        queues.append(queue)
    
    # Start all processes
    for p in processes:
        p.start()
    
    # Wait for all processes to complete
    for p in processes:
        p.join()
    
    # Check for any exceptions
    for q in queues:
        while not q.empty():
            result = q.get()
            if isinstance(result, Exception):
                raise result
    
    # Verify data in database
    writer = SQLiteWriter(db_path)
    with writer.get_cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {TESTS_TABLE}")
        count = cursor.fetchone()[0]
        assert count == num_processes  # Each process should have written one row