# Python program to convert text
# file to JSON
import json

# the file to be converted to
# json format
import sys

filename = sys.argv[1]
# creating dictionary
l=''
lt=''
lu=''
lw=''
with open(filename,"r",encoding='utf-8') as fh:
    for line in fh:
        if line.startswith("T"):
            ident = line[0]
            lt += ident + " " + line[1:].strip()

        elif line.startswith("U"):
            ident = line[0]
            line = line[21:]
            lu += ident + " " + line.strip()

        elif line.startswith("W"):
            ident = line[0]
            if line[2:].startswith("No Post Title"):
                l = ''
                lt = ''
                lu = ''
                lw = ''
            else:
                lw += ident + " " + line[2:].strip()
                l = lt + "," + lu + "," + lw + "\n"
                if len(l) > 2:
                    print(l)
                    with open(sys.argv[2], "a", encoding='utf-8') as out_file:
                        out_file.write(l)
                        l = ''
                        lt = ''
                        lu = ''
                        lw = ''



