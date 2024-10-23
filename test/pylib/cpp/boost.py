import io
import os
import subprocess
from pathlib import Path
from subprocess import TimeoutExpired
from typing import Sequence, Tuple
from xml.etree import ElementTree

Markup = Tuple[str, ...]

class BoostTestFailure(Exception):
    def __init__(self, filename: str, linenum: int, contents: str) -> None:
        self.filename = filename
        self.linenum = linenum
        self.lines = contents.splitlines()

    def get_lines(self) -> list[tuple[str, Markup]]:
        m = ("red", "bold")
        return [(x, m) for x in self.lines]

    def get_file_reference(self) -> tuple[str, int]:
        return self.filename, self.linenum

class BoostFailureError(Exception):
    def __init__(self, failures: Sequence[BoostTestFailure]) -> None:
        self.failures = list(failures)

class BoostTestFacade:
    """
    Facade for BoostTests.
    """

    temp_dir: Path = None
    mode: str = None

    def __init__(self, mode:str, temp_dir=Path('./testlog')):
        self.mode = mode
        self.temp_dir = temp_dir

    @staticmethod
    def list_tests(
        executable: str,
        no_parallel: bool,
    ) -> list[str]:
        if no_parallel:
            return [os.path.basename(os.path.splitext(executable)[0])]
        else:
            args = [executable, '--list_content']
            try:
                output = subprocess.check_output(
                    args,
                    stderr=subprocess.STDOUT,
                    universal_newlines=True,
                )
            except subprocess.CalledProcessError as e:
                output = e.output
            # --list_content produces the list of all test cases in the file. When BOOST_DATA_TEST_CASE is used it
            # will additionally produce the lines with numbers for each case preserving the function name like this:
            # test_singular_tree_ptr_sz*
            #     _0*
            #     _1*
            #     _2*
            # however, it's only possible to run test_singular_tree_ptr_sz that will execute all test cases
            # this line catches only test function name ignoring unrelated lines like '_0'
            # Note: this will ignore any test case starting with a '_' symbol
            return [case[:-1] for case in output.splitlines() if
                         case.endswith('*') and not case.strip().startswith('_')]

    def run_test(
        self,
        executable: str,
        test_id: str,
        test_args: Sequence[str] = (),
    ) -> tuple[Sequence[BoostTestFailure] | None, str]:
        def read_file(name: Path) -> str:
            try:
                with io.open(name) as f:
                    return f.read()
            except IOError:
                return ""

        stdout = ''
        stderr = ''
        timeout = 60 * 2 if self.mode=='debug' else 60 * 2
        log_xml = self.temp_dir / self.mode / 'pytest' / f'{test_id}.log.xml'
        report_xml = self.temp_dir / self.mode / 'pytest' / f'{test_id}.xml'
        old_xml = self.temp_dir / self.mode / 'xml' / f'boost.{test_id}.xunit.xml'
        args = [ executable,
                '--output_format=XML',
                f'--log_sink={log_xml}',
                f'--report_sink={report_xml}',
                '--catch_system_errors=no',
                '--report_level=no',
                '--color_output=false',
                f'--logger=HRF,test_suite:XML,test_suite,{old_xml}',
                 *test_args,
                 ]
        os.chdir(self.temp_dir.parent)
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=1, universal_newlines=True)
        try:
            subprocess.PIPE
            raw_stdout, raw_stderr = p.communicate(timeout=timeout)
            # stdout = raw_stdout.decode("utf-8") if raw_stdout else ""
            # stderr = raw_stderr.decode("utf-8") if raw_stderr else ""
        except TimeoutExpired:
            print('Timeout reached')
            p.kill()
        except KeyboardInterrupt as e:
            p.kill()
            raise e

        log = read_file(log_xml)
        report = read_file(report_xml)

        if p.returncode != 0:
            msg = (
                "working_dir: {working_dir}\n"
                "Internal Error: calling {executable} "
                "for test {test_id} failed (returncode={returncode}):\n"
                "output:{stdout}\n"
                "std error:{stderr}\n"
                "log:{log}\n"
                "report:{report}"
                "command to repeat:{command}"
            )
            failure = BoostTestFailure(
                "<no source file>",
                linenum=0,
                contents=msg.format(
                    working_dir=os.getcwd(),
                    executable=executable,
                    test_id=test_id,
                    stdout=raw_stdout,
                    stderr=raw_stderr,
                    log=log,
                    report=report,
                    command=' '.join(p.args),
                    returncode=p.returncode,
                ),
            )
            return [failure], stdout

        results = self._parse_log(log=log)

        if results:
            return results, stdout

        return None, stdout

    def _parse_log(self, log: str) -> list[BoostTestFailure]:
        """
        Parse the "log" section produced by BoostTest.

        This is always a XML file, and from this we produce most of the
        failures possible when running BoostTest.
        """
        # Boosttest will sometimes generate unparseable XML
        # so we surround it with xml tags.
        parsed_elements = []
        log = "<xml>{}</xml>".format(log)

        log_root = ElementTree.fromstring(log)
        testlog = log_root.find("TestLog")

        parsed_elements.extend(log_root.findall("Exception"))
        parsed_elements.extend(log_root.findall("Error"))
        parsed_elements.extend(log_root.findall("FatalError"))

        if testlog is not None:
            parsed_elements.extend(testlog.findall("Exception"))
            parsed_elements.extend(testlog.findall("Error"))
            parsed_elements.extend(testlog.findall("FatalError"))

        result = []
        for elem in parsed_elements:
            filename = elem.attrib["file"]
            linenum = int(elem.attrib["line"])
            result.append(BoostTestFailure(filename, linenum, elem.text or ""))
        return result
