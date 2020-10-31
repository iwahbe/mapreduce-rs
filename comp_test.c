#include "mapreduce.h"
#include <stdio.h>
#include <stdlib.h>

void reduce(char *key, Getter get_func, int partition_number) {
  printf("reduce called with key: '%s', partition_numer: '%d'\n", key,
         partition_number);
  char *value;
  while ((value = get_func("get_func", partition_number)) != NULL)
    printf("reduce found key, value pair (%s, %s)\n", key, value);
}

unsigned long partition(char *key, int num_partition) {
  printf("partition called on key: '%s', num_partition: '%d'\n", key,
         num_partition);
  return 0;
}

void map(char *file_name) {
  printf("map called with file_name: '%s'\n", file_name);
  MR_Emit(file_name, file_name);
}

int main(int argc, char **argv) {
  if (argc < 2) {
    printf("comp_test: [keys+]\n");
    exit(1);
  }

  int num_mappers = 2;
  int num_reducers = 5;
  MR_Run(argc, argv, map, num_mappers, reduce, num_reducers, partition);
}
