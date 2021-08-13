file_name = '../data/datafile'
with open(file_name, 'r') as f:
  lines = f.readlines()
  lines.sort()
  with open("./out_py", 'w') as ff:
    for idx, line in enumerate(lines):
      print("{}. {}".format(idx+1, line.strip()))
      ff.write(line)
