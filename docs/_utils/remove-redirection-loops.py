# -*- coding: utf-8 -*-

# update redirection file to remove double redireaction
# a->b, b->c, is rewritten ad a->c,b->c
# write result to stdout

import os
import sys
import re
import yaml

f = './redirections.yaml'
new_f = './redirections.new.yaml'
d = yaml.load(open(f), Loader=yaml.FullLoader)
replace_dict = {}
for key, value in d.items():
     if value in d.keys():
#        print ("double redirection:", key, "->", value, "->",  d[value])
         replace_dict[value] = d[value]

with open(f, "r") as source:
    lines = source.readlines()
    for line in lines:
        dline = yaml.safe_load(line)
        if (dline is not None):
             for key, value in dline.items():
                  if value in replace_dict.keys():
                       print (key, ": ", replace_dict[value],sep="")
                  else:
                       print(line, end='')
        else:
             print(line, end='')

