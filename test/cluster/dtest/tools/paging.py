import logging
import time
import traceback
import unittest

from tools.datahelp import flatten_into_set

logger = logging.getLogger(__name__)


class Page:
    data = None

    def __init__(self):
        self.data = []

    def add_row(self, row):
        self.data.append(row)


class PageFetcher:
    """
    Requests pages, handles their receipt,
    and provides paged data for testing.

    The first page is automatically retrieved, so an initial
    call to request_one is actually getting the *second* page!
    """

    pages = None
    error = None
    future = None
    requested_pages = None
    retrieved_pages = None
    retrieved_empty_pages = None

    def __init__(self, future):
        self.pages = []

        # the first page is automagically returned (eventually)
        # so we'll count this as a request, but the retrieved count
        # won't be incremented until it actually arrives
        self.requested_pages = 1
        self.retrieved_pages = 0
        self.retrieved_empty_pages = 0

        self.future = future
        self.future.add_callbacks(callback=self.handle_page, errback=self.handle_error)

        # wait for the first page to arrive, otherwise we may call
        # future.has_more_pages too early, since it should only be
        # called after the first page is returned
        self.wait(seconds=30)

    def handle_page(self, rows):
        # occasionally get a final blank page that is useless
        if rows == []:
            self.retrieved_empty_pages += 1
            return

        page = Page()
        self.pages.append(page)

        for row in rows:
            page.add_row(row)

        self.retrieved_pages += 1

    def handle_error(self, exc):
        self.error = exc
        raise exc

    def request_one(self, timeout=None):
        """
        Requests the next page if there is one.

        If the future is exhausted, this is a no-op.
        @param timeout Time, in seconds, to wait for all pages.
        """
        if self.future.has_more_pages:
            self.future.start_fetching_next_page()
            self.requested_pages += 1
            self.wait(seconds=timeout)

        return self

    def request_all(self, timeout=None):
        """
        Requests any remaining pages.

        If the future is exhausted, this is a no-op.
        @param timeout Time, in seconds, to wait for all pages.
        """
        while self.future.has_more_pages:
            self.future.start_fetching_next_page()
            self.requested_pages += 1
            self.wait(seconds=timeout)

        return self

    def wait(self, seconds=None):
        """
        Blocks until all *requested* pages have been returned.

        Requests are made by calling request_one and/or request_all.

        Raises RuntimeError if seconds is exceeded.
        """
        seconds = 5 if seconds is None else seconds
        expiry = time.time() + seconds

        while time.time() < expiry:
            if self.requested_pages == (self.retrieved_pages + self.retrieved_empty_pages):
                return self
            # small wait so we don't need excess cpu to keep checking
            time.sleep(0.1)

        raise RuntimeError("Requested pages were not delivered before timeout. " + f"Requested: {self.requested_pages}; retrieved: {self.retrieved_pages}; empty retrieved: {self.retrieved_empty_pages}")

    def pagecount(self):
        """
        Returns count of *retrieved* pages which were not empty.

        Pages are retrieved by requesting them with request_one and/or request_all.
        """
        return len(self.pages)

    def num_results(self, page_num):
        """
        Returns the number of results found at page_num
        """
        return len(self.pages[page_num - 1].data)

    def num_results_all(self):
        return [len(page.data) for page in self.pages]

    def page_data(self, page_num):
        """
        Returns retreived data found at pagenum.

        The page should have already been requested with request_one and/or request_all.
        """
        return self.pages[page_num - 1].data

    def all_data(self):
        """
        Returns all retrieved data flattened into a single list (instead of separated into Page objects).

        The page(s) should have already been requested with request_one and/or request_all.
        """
        all_pages_combined = []
        for page in self.pages:
            all_pages_combined.extend(page.data[:])

        return all_pages_combined

    @property  # make property to match python driver api
    def has_more_pages(self):
        """
        Returns bool indicating if there are any pages not retrieved.
        """
        return self.future.has_more_pages


class PageAssertionMixin:
    """Can be added to subclasses of unittest.Tester"""

    @staticmethod
    def assert_equal_ignore_order(actual, expected, msg=None):
        if msg:
            msg = f"{msg}: expected {expected} but got {actual}"
        unittest.TestCase().assertCountEqual(expected, actual, msg)

    @staticmethod
    def assert_is_subset_of(subset, superset):
        assert flatten_into_set(subset) <= flatten_into_set(superset)


class MultiError(Exception):
    """
    Extends Exception to provide reporting multiple exceptions at once.
    """

    def __init__(self, exceptions, tracebacks):
        # an exception and the corresponding traceback should be found at the same
        # position in their respective lists, otherwise __str__ will be incorrect
        self.exceptions = exceptions
        self.tracebacks = tracebacks

    def __str__(self):
        output = "\n****************************** BEGIN MultiError ******************************\n"

        for exc, tb in zip(self.exceptions, self.tracebacks):
            output += str(exc)
            output += tb + "\n"

        output += "****************************** END MultiError ******************************"

        return output


def run_scenarios(scenarios, handler):
    """
    Runs multiple scenarios from within a single test method.

    "Scenarios" are mini-tests where a common procedure can be reused with several different configurations.
    They are intended for situations where complex/expensive setup isn't required and some shared state is
    acceptable (or trivial to reset).

    Arguments: scenarios should be an iterable, handler should be a callable, and deferred_exceptions should
    be a tuple of exceptions which are safe to delay until the scenarios are all run. For each item in scenarios,
    handler(item) will be called in turn.

    Exceptions which occur will be bundled up and raised as a single MultiError exception, either when:
        a) all scenarios have run, or
        b) on the first exception encountered which is not AssertionError.
    """
    errors = []
    tracebacks = []
    num_of_scenarios = len(scenarios)

    for i, scenario in enumerate(scenarios, start=1):
        logger.info("running scenario %s/%s: %s", i, num_of_scenarios, scenario)
        try:
            handler(*scenario)
        except Exception as exc:  # noqa: BLE001
            tracebacks.append(traceback.format_exc())
            errors.append(type(exc)(f"encountered {exc.__class__.__name__} {exc} running scenario:\n  {scenario}\n"))
            if not isinstance(exc, AssertionError):
                logger.info("scenario %s/%s encountered a non-deferrable exception, aborting", i, num_of_scenarios)
                break
            logger.info("scenario %s/%s encountered a deferrable exception, continuing", i, num_of_scenarios)

    if errors:
        raise MultiError(errors, tracebacks)
