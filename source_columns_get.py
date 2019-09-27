#used to parse through data file and retrive source names
#replace sample_list_of_sources.dat, with said data file

import sys

list = []

with open("sample_list_of_sources.dat", "r") as file:
	for line in file:
		(one, two, three) = line.partition(":")
		three=three.strip()
		three = three.strip(',')
		three = three.strip('"')
		list.append(three)

	print list
