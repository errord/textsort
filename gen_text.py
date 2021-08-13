#! /usr/bin/python3

import random

#FILE_SIZE = 200000000
FILE_SIZE = 100

def random_str():
  str_len = random.randint(1, 255)
  s = ''.join(random.choices(''.join([chr(i) for i in range(32, 127)]), k=str_len))
  assert len(s) <= 255, "random string length: {} > 255".format(len(s))
  return s

def main():
  with open("datafile", 'w') as f:
    for _ in range(FILE_SIZE):
      f.write(random_str()+"\n")

if __name__ == "__main__":
  main()
