import fileinput
f = open("small.txt",'w+')
for line in fileinput.input("sample-small.txt"):
	strs = line.split(" ")
	f.write(strs[0]+"\t"+str(1))
	count = 1
	for i in range(1,len(strs)):
		f.write("\t"+strs[i])
	f.write("\n")
