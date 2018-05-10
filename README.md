# MapReduceWordCounting
Map Reduce word counting program by using MPI and OpenMP

Program will count words of all files in folder RawText. And generate output .txt files based on number of node and number of threads.

sendFileByFile indicates the sender of each node will send tuples after finished each file instead of whole folder.

To do:
  1. Reorganize the code.
  2. Get directory from user instead of hard coding.
  3. Merge the final result into 1 txt file.
