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