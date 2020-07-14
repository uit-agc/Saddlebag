#!/usr/bin/python

# Copyright 2019. Aril B Ovesen, Amin M Khan

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys

def main(argv):

	mydir = "wikidump/"
	mylist = "filenames.txt"

	if len(argv) > 0:
		mydir = argv[0]

	if len(argv) > 1:
		mylist = argv[1]

	print("Processing " + mydir + " ...")
	
	files = os.listdir(mydir)

	with open(mylist, "w") as text_file:
		text_file.write(str(len(files)) + " ")
		for f in files:
			 text_file.write(f + " ")

	print("Finished processing " + str(len(files)) + " files, created: " + mylist)


if __name__ == "__main__":
	main(sys.argv[1:])
