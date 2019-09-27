#!/usr/bin/env python
# -*- coding: latin-1 -*-
"""reducer.py"""
#reducer - takes output from mapper as input, parses, & combines the data tuples into smaller set which is the final output
#reducer - prevents duplicates in keys

from itertools import groupby
import sys


#confidential information has been removed from file (sourcenames), replaces with 'source1', 'source2', 'source3'
SOURCE_COLUMNS = ['source1', 'source2', 'source3']

previous_key = None
running_source_set = set()
previous_touchpoint_type = None


#input comes from STDIN
for line in sys.stdin:
    #remove leading and trailing whitespace
    line = line.strip()
    #parse the input
    key, tab, value = line.partition('\t')
    #parse out value from touchpoint_type
    value, tab, touchpoint_type = value.partition('\t')
    #split
    value = value.split(',')


    record_key = key
    record_value = value
    record_touchpoint_type = touchpoint_type


    if record_key == previous_key:
        #sources(record_value) added to running_source_set for key
        #running_source_set |= set(record_value)
        for value in record_value:
            running_source_set.add(value)


    elif previous_key == None and previous_touchpoint_type == None:
        #running_source_set.add(record_value)
        for value in record_value:
            running_source_set.add(value)


    elif record_key != previous_key:

        matches = []
        for word in SOURCE_COLUMNS:
            if word in running_source_set:
                matches.append('1')
            else:
                matches.append('0')


        #print previous_key + '\t' + previous_touchpoint_type + '\t' + str(matches)      #replace with ('\t'.join(matches)) to seperate with tab
        print previous_key + '\t' + previous_touchpoint_type + '\t' + ('\t'.join(matches)) #<--change int in matches to string 
        #print previous_key + '\t' + ','.join(running_source_set) + '\t' + previous_touchpoint_type     #<-- #used in testing key,value, touchpoint_type
        
        #reset running_source_set as set()
        #clear out old sources, begin with new key
        running_source_set = set()

        for value in record_value:
            running_source_set.add(value)


    previous_key = record_key
    previous_touchpoint_type = record_touchpoint_type


if previous_key is not None:
    #print out the last line
    #print previous_key + '\t' + previous_touchpoint_type + '\t'+ str(matches)       #replace with ('\t'.join(matches)) to seperate with tab
    print previous_key + '\t' + previous_touchpoint_type + '\t' + ('\t'.join(matches))
    #print previous_key + '\t' + ','.join(running_source_set) + '\t' + previous_touchpoint_type     #<-- #used in testing key,value, touchpoint_type
