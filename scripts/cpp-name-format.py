#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2019-present ScyllaDB
#

#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
#

import argparse
import io
import os
import sys
import os.path as path
import re
from enum import Enum
from collections import deque

ap = argparse.ArgumentParser(description='Pretty-print C++ names.')
ap.add_argument('--indent', dest='indent', default='  ', help='Indentation')
ap.add_argument('--break', dest='maxwidth', default=32, nargs='?', const=1, help='Break tokens longer than this parameter into multiple lines')
ap.add_argument('--debug', dest='debug', type=int, default=0, nargs='?', const=1, help='Debugging level')
ap.add_argument('strings', metavar='S',  type=str, nargs='*', help='File(s) or strings to parse')

args = ap.parse_args()

def debug(s, level=1):
    if level <= args.debug:
        print('DEBUG[{}] {}'.format(level, s))

class Token:
    class Type(Enum):
        Empty = 0
        Word = 1
        Space = 2
        Comma = 3
        Open = 4
        Close = 5
        Compound = 6

    def __init__(self, content, type, depth):
        self.content = content
        self.type = type
        self.depth = depth
        self.length = None

    def __repr__(self):
        return "Token:<{} depth={} len={} content='{}'>".format(self.type, self.depth, self.len(), self.content)

    def measure(self):
        if self.type != Token.Type.Compound:
            return len(self.content)
        l = 0
        for t in self.content:
            l += t.len()
        return l

    def len(self):
        if self.length is None:
            self.length = self.measure()
        return self.length

    def singleline(self):
        return (self.type != Token.Type.Compound and self.type != Token.Type.Word) or \
               self.len() <= int(args.maxwidth)

    def indentation(self, indent=None):
        if not indent:
            indent = args.indent
        return '\n' + indent * self.depth

    def format(self, depth=0, nested=False):
        debug("Format depth={} nested={} singleline={} {}".format(depth, nested, self.singleline(), self))
        s = ''
        if self.type != Token.Type.Compound:
            if not self.singleline() and nested:
                s = self.indentation()
            return s + self.content

        if self.singleline():
            for t in self.content:
                s += t.format(depth+1, nested=nested)
            return s

        last = None
        newl = False
        indentation = self.indentation()
        indent = True
        opened = False
        for t in self.content:
            if t.type == Token.Type.Space and \
               (indent or (last and last.type == Token.Type.Comma)):
                continue
            last = t
            if indent and t.type != Token.Type.Comma:
                s += indentation
            indent = False

            s += t.format(depth=depth+1, nested=opened)

            if t.type == Token.Type.Comma or (opened and not t.singleline()):
                indent = True

            if t.type == Token.Type.Open:
                opened = True
            elif t.type == Token.Type.Close:
                opened = False
        return s

    class Parser:
        def __init__(self):
            self.delimiters = re.compile(r'(?P<space>\s+)|(?P<open>[\[\(\{<])|(?P<close>[>\}\)\]])|(?P<comma>,)')
            self.lex_map = {
                'space': Token.Type.Space,
                'open':  Token.Type.Open,
                'close': Token.Type.Close,
                'comma': Token.Type.Comma,
            }
            self.queue = None

        def debug_token(self, desc, t, debug_level=2):
            debug("{} {}".format(desc, t), debug_level)
            return t

        def lex(self, line):
            depth = 0
            while line:
                m = self.delimiters.search(line)
                if not m:
                    yield self.debug_token("Yield line", Token(line, Token.Type.Word, depth))
                    break
                if m.start():
                    yield self.debug_token("Yield", Token(line[:m.start()], Token.Type.Word, depth))
                token_type = self.lex_map[m.lastgroup]
                if token_type == Token.Type.Close:
                    depth -= 1
                yield self.debug_token("Yield", Token(m[0], token_type, depth))
                if token_type == Token.Type.Open:
                    depth += 1
                line = line[m.end():]

        def parse_tokens(self, depth=0):
            tokens = deque()
            while self.queue:
                t = self.queue[0]
                if t.depth < depth:
                    if tokens and tokens[len(tokens)-1].type == Token.Type.Space:
                        tokens.pop()
                    break
                elif t.depth > depth:
                    t = self.parse_tokens(t.depth)
                    tokens.append(self.debug_token("Appending nested depth={}".format(depth), t, 3))
                else:
                    self.queue.popleft()
                    tokens.append(self.debug_token("Appending depth={}".format(depth), t, 3))

            if tokens and tokens[0].type == Token.Type.Space:
                tokens.popleft()
            if tokens and tokens[len(tokens)-1].type == Token.Type.Space:
                tokens.pop()
            if not tokens:
                ret = Token('', Token.Type.Empty, depth)
            elif len(tokens) == 1:
                ret = tokens[0]
            else:
                ret = Token(tokens, Token.Type.Compound, depth)
            return self.debug_token("Return depth={}".format(depth), ret)

        def parse(self, line):
            debug("Parse: '{}'".format(line), 1)
            self.queue = deque(Token.Parser().lex(line))
            return self.parse_tokens()

def parse_line(line):
    print(Token.Parser().parse(line).format())

def parse_file(f):
    for l in f:
        parse_line(l.strip())

if args.strings:
    for s in args.strings:
        if s == '-':
            parse_file(sys.stdin)
        elif path.exists(s):
            f = open(s, "r")
            parse_file(f)
            f.close()
        else:
            parse_line(s)
else:
    parse_file(sys.stdin)
