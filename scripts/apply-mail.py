#!/usr/bin/python3

import email
import argparse
import re
import tempfile
import os

argparser = argparse.ArgumentParser()
argparser.add_argument('emails', metavar='FILE', nargs='+',
                            help='Files with email message containing a patch')


args = argparser.parse_args()

bad_google = re.compile(r'".* via .*" <.*@googlegroups.com>')

temps = []

for email_file in args.emails:
    e = email.message_from_file(open(email_file))
    author = e.get('From')
    m = re.fullmatch(bad_google, author)
    if m:
        e.replace_header('From', e.get('Reply-To'))
    o = tempfile.NamedTemporaryFile()
    o.write(bytes(e))
    temps.append(o)

names = str.join(' ', [f.name for f in temps])

os.system(f'git am -3mi {names}')

    
    
