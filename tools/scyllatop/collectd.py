import socket
import re
import atexit
import os
import parseexception
import logging

COLLECTD_EXAMPLE_CONFIGURATION = '\n'.join(['LoadPlugin unixsock',
                                            '',
                                            '<Plugin unixsock>',
                                            '   SocketFile "{socket}"',
                                            '   SocketGroup "wheel"',
                                            '   SocketPerms "0660"',
                                            '   DeleteSocket false',
                                            '</Plugin>'])


class Collectd(object):
    _FIRST_LINE_PATTERN = re.compile('^(?P<lines>\d+)')
    _METRIC_INFO_PATTERN = re.compile('^(?P<key>[^=]+)=(?P<value>.*)$')
    _METRIC_DISCOVER_PATTERN_WITH_HELP = re.compile('[^ ]+ (?P<metric>.+)(?P<help>.*)$')
    _METRIC_DISCOVER_PATTERN = re.compile('[^ ]+ (?P<metric>.+)(?P<help>.*)$')

    def __init__(self, socketName):
        try:
            self._connect(socketName)
            atexit.register(self._cleanup)
        except Exception as e:
            logging.error('could not connect to {0}. {1}'.format(socketName, e))
            quit()

    def _connect(self, socketName):
        logging.info('connecting to unix socket: {0}'.format(socketName))
        self._socket = socket.socket(socket.SOCK_STREAM, socket.AF_UNIX)
        self._socket.connect(socketName)
        self._lineReader = os.fdopen(self._socket.fileno())

    def query_val(self, val):
        return self.internal_query('GETVAL "{metric}"'.format(metric=val))

    def query_list(self):
        return self.internal_query('LISTVAL')

    def internal_query(self, command):
        self._send(command)
        return self._readLines()

    def _send(self, command):
        withNewline = '{command}\n'.format(command=command)
        octets = withNewline.encode('ascii')
        self._socket.send(octets)

    def _readLines(self):
        line = self._lineReader.readline()
        if line.strip() == '-1 No such value':
            return None
        match = self._FIRST_LINE_PATTERN.search(line)
        if match is None:
            raise parseexception.ParseException('could not parse first line of response from collectd: {0}'.format(line))
        howManyLines = int(match.groupdict()['lines'])
        return [self._lineReader.readline() for _ in range(howManyLines)]

    def _cleanup(self):
        self._lineReader.close()
        self._socket.close()
