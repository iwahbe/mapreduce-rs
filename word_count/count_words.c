#include "../mapreduce.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void Map(char *file_name) {
  FILE *fp = fopen(file_name, "r");
  assert(fp != NULL);

  char *line = NULL;
  size_t size = 0;
  while (getline(&line, &size, fp) != -1) {
    char *token, *dummy = line;
    while ((token = strsep(&dummy, " \t\n\r")) != NULL) {
      MR_Emit(token, "1");
    }
  }
  free(line);
  fclose(fp);
}

void Reduce(char *key, Getter get_next, int partition_number) {
  int count = 0;
  char *value;
  while ((value = get_next(key, partition_number)) != NULL)
    count++;
  printf("%s %d\n", key, count);
}

int main(int argc, char *argv[]) {
  printf("argc %d\n", argc);
  assert(argc > 3);
  int num_map, num_part;
  assert(num_map = atoi(argv[1]));
  assert(num_part = atoi(argv[2]));

  MR_Run(argc - 2, argv + 2, Map, num_map, Reduce, num_part,
         MR_DefaultHashPartition);
}
