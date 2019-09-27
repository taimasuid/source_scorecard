#!/usr/bin/env python
# -*- coding: latin-1 -*-
import sys

#fieldnames file used to create list of fieldnames required for sourceScorecard_bq_layout.py

#confidential information is removed from file 
#inclide source names in SOURCE_COLUMNS
SOURCE_COLUMNS = ['source1', 'source2', 'source3']

 # ex: "fieldName": "00007"

for word in SOURCE_COLUMNS:
	print "{"
	print '"fieldName"'+": " + '%s'%('"'+word+'"')
	print "},"
