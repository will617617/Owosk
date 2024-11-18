#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>

#define NUM_COLUMNS 4
#define MAX_ROWS 1000
#define MAX_LINE_LENGTH 256

typedef struct {
    int column;
    int *data;
    int size;
    long sum;
} ThreadData;

// Comparison function for qsort
int compare(const void *a, const void *b) {
    return (*(int *)a - *(int *)b); // Return the difference between two integers
}

void *sort_and_sum(void *arg) {
    ThreadData *td = (ThreadData *)arg;
    int column = td->column;
    int *data = td->data;
    int size = td->size;

    // Sort the column data
	qsort(data, size, sizeof(int), compare);

    // Calculate the sum
    td->sum = 0;
    for (int i = 0; i < size; i++)
        td->sum += data[i];

    pthread_exit(NULL);
}

void *merge_columns(void *arg) {
    ThreadData *threads_data = (ThreadData *)arg;
    int total_size = 0;

    // Calculate total size for merged array
    for (int i = 0; i < NUM_COLUMNS; i++)
        total_size += threads_data[i].size;

    int *merged_data = (int *)malloc(total_size * sizeof(int));
    int index = 0;

    // Merge sorted columns
    for (int i = 0; i < NUM_COLUMNS; i++) {
        memcpy(merged_data + index, threads_data[i].data, threads_data[i].size * sizeof(int));
        index += threads_data[i].size;
    }

    // Sort merged data
	qsort(merged_data, total_size, sizeof(int), compare);

    // Output merged data to file
    FILE *output_file = fopen("output.txt", "a");
    
    // Write merged results in specified format
    fprintf(output_file, "=================================================\n");
    
    for (int i = 0; i < total_size; i++) {
        fprintf(output_file, "%d", merged_data[i]);
        if ((i + 1) % 40 == 0) fprintf(output_file, "\n");
        else if (i != total_size - 1) fprintf(output_file, ", ");
    }
    
    fclose(output_file);
    
    free(merged_data);
    
    pthread_exit(NULL);
}

void read_input(const char *filename, int data[MAX_ROWS][NUM_COLUMNS], int *row_count) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        perror("File opening failed");
        exit(EXIT_FAILURE);
    }

    *row_count = 0;
    
    while (*row_count < MAX_ROWS && fscanf(file, "%d,%d,%d,%d\n", 
           &data[*row_count][0], &data[*row_count][1], 
           &data[*row_count][2], &data[*row_count][3]) == NUM_COLUMNS) {
        (*row_count)++;
    }
    
    fclose(file);
}

int main() {
    char input_filename[100];	
    
    printf("Enter the source file name: ");
    scanf("%s", input_filename);

    int data[MAX_ROWS][NUM_COLUMNS];
    int row_count;

    read_input(input_filename, data, &row_count);
    
    FILE *output_file = fopen("output.txt", "w");
    // Clear previous content of output file
    fclose(output_file);
    
    pthread_t threads[NUM_COLUMNS];
    ThreadData thread_data[NUM_COLUMNS];
	
    for (int col = 0; col < NUM_COLUMNS; col++) {
        thread_data[col].column = col;
        thread_data[col].size = row_count;
        thread_data[col].data = (int *)malloc(row_count * sizeof(int));
        
        for (int row = 0; row < row_count; row++) {
            thread_data[col].data[row] = data[row][col];
        }
		
        pthread_create(&threads[col], NULL, sort_and_sum, &thread_data[col]);
        
        // Wait for each thread to finish
        pthread_join(threads[col], NULL);

        // Write sorted column and sum to output file
        output_file = fopen("output.txt", "a");
        fprintf(output_file, "Thread %d: ", col + 1);
        
        for (int row = 0; row < row_count; row++) {
            fprintf(output_file, "%d", thread_data[col].data[row]);
            if (row != row_count - 1) fprintf(output_file, ", ");
        }
        
        fprintf(output_file, "\nsum %d: %ld\n", col + 1, thread_data[col].sum);
        
        fclose(output_file);
        
    }

    pthread_t merge_thread;
    
    pthread_create(&merge_thread, NULL, merge_columns, thread_data);
    
    pthread_join(merge_thread, NULL);

    return 0;
}
