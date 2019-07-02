// V. Freitas [2018] @ ECL-UFSC

// INE5410 - Cuncurrent Programming
// Gustavo Kundlatsch and João Fellipe Uller Project
#include <stdlib.h>
#include <time.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <stdarg.h>
#include <mpi.h>

#ifndef DEBUG
#define DEBUG 0
#endif

#ifndef NELEMENTS
#define NELEMENTS 6
#endif

#ifndef MAXVAL
#define MAXVAL 255
#endif // MAX_VAL

/*
* More info on: http://en.cppreference.com/w/c/language/variadic
*/

void debug(const char* msg, ...) {
	if (DEBUG > 3) {
		va_list args;
		va_start(args, msg);
		vprintf(msg, args);
		va_end(args);
	}
}

/*
* Orderly merges two int arrays (numbers[begin..middle] and numbers[middle..end]) into one (sorted).
* \retval: merged array -> sorted
*/
void merge(int* numbers, int begin, int middle, int end, int* sorted) {
	int i, j;
	i = begin; j = middle;
	debug("Merging. Begin: %d, Middle: %d, End: %d\n", begin, middle, end);
	for (int k = begin; k < end; k++) {
		debug("LHS[%d]: %d, RHS[%d]: %d\n", i, numbers[i], j, numbers[j]);
		if (i < middle && (j >= end || numbers[i] < numbers[j])) {
			sorted[k] = numbers[i];
			i++;
		} else {
			sorted[k] = numbers[j];
			j++;
		}
	}
}


/*
* Merge sort recursive step
*/
void recursive_merge_sort(int* tmp, int begin, int end, int* numbers) {
	if (end - begin < 2)
		return;
	else {
		int middle = (begin + end)/2;
		recursive_merge_sort(numbers, begin, middle, tmp);
		recursive_merge_sort(numbers, middle, end, tmp);
		merge(tmp, begin, middle, end, numbers);
	}
}

// Special merge, used to merge two different arrays
// The original merge use only one array
void merge_sorted_arrays(int* a, int* b, int a_size, int b_size, int* tmp) {
	int i = 0; int j = 0;
	for (int k = 0; k < (a_size + b_size); k++) {
		if (i < a_size && (j >= b_size || a[i] < b[j])) {
			tmp[k] = a[i];
			i++;
		} else {
			tmp[k] = b[j];
			j++;
		}
	}
}

// First Merge Sort call (Need memory use revision)
void merge_sort(int* a, int* b, int a_size, int b_size, int* tmp) {
	// Vetor aux is used in the sort function
	int* aux = malloc(a_size * sizeof(int));
	memcpy(aux, a, a_size*sizeof(int));
	recursive_merge_sort(a, 0, a_size, aux);
	memcpy(a, aux, a_size * sizeof(int));
	
	// Only reallocate memory if b's size is odd
	if((b_size % 2) != 0)
		aux = realloc(aux,b_size * sizeof(int));
	memcpy(aux, b, b_size*sizeof(int));
	recursive_merge_sort(b, 0, b_size, aux);
	memcpy(b, aux, b_size * sizeof(int));

	merge_sorted_arrays(a, b, a_size, b_size, tmp);
}


void print_array(int* array, int size) {
	printf("Printing Array:\n");
	for (int i = 0; i < size; ++i) {
		printf("%d. ", array[i]);
	}
	printf("\n");
}

void populate_array(int* array, int size, int max) {
	int m = max+1;
	for (int i = 0; i < size; ++i) {
		array[i] = rand()%m;
	}
}

void divide_array(int arr_size, int* a_size, int* b_size) {
	if (arr_size % 2 == 0) {
		*a_size = arr_size/2;
		*b_size = *a_size;
	} else {
		*a_size = (int)arr_size/2;
		*b_size = *a_size + 1;  // In odd entries, b gets the extra element
	}
}

void split_array(int* array, int* a, int* b, int a_size, int b_size) {
	for (int i = 0; i < a_size; i++) {
		a[i] = array[i];
	}

	for (int i = 0; i < b_size; i++) {
		b[i] = array[i + a_size];
	}
}

// Return initial level of a process in the tree based on its rank
int get_initial_level(int rank) {
	if (rank == 0)
		return 0;
	else
		return ((int) log2((double) rank)) + 1;
}

// Get the rank of a subprocess based on the actual process and the level it is on the tree
int get_son_rank(int rank, int tree_level) {
	return rank + pow(2, (double) tree_level);
}

int main (int argc, char ** argv) {
	// MPI variables
	int comm_world_size, rank;
	MPI_Status st;

	// Each process local variables
	int tree_level, father_rank;
	size_t arr_size;  // tmp size
	int *a, *b, a_size, b_size, *tmp, aux[1];

	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &comm_world_size);

	tree_level = get_initial_level(rank);

	// Main process
	if (rank == 0) {
		int seed, max_val;
		int* sortable;

		// RECURSIVE-MERGE-SORT unit test
		if (DEBUG > 2) {
			a = (int*)malloc(8*sizeof(int));
			a[0] = 1; a[1] = 3; a[2] = 4; a[3] = 7;
			a[4] = 0; a[5] = 2; a[6] = 5; a[7] = 6;

			tmp = (int*) malloc(8*sizeof(int));

			print_array(a, 8);
			recursive_merge_sort(a, 0, 8, tmp);
			print_array(tmp, 8);
			return 3;
		}

		// Basic MERGE unit test
		if (DEBUG > 1) {
			a = (int*)malloc(8*sizeof(int));
			a[0] = 1; a[1] = 3; a[2] = 4; a[3] = 7;
			a[4] = 0; a[5] = 2; a[6] = 5; a[7] = 6;
			int * values = (int*)malloc(8*sizeof(int));
			merge(a, 0, 4, 8, values);
			free (a);
			print_array(values, 8);
			free(values);
			return 2;
		}

		// Basic MERGE-SORT unit test
		if (DEBUG > 0) {
			a = (int*)malloc(8*sizeof(int));
			b = (int*)malloc(8*sizeof(int));
			tmp = (int*)malloc(16*sizeof(int));
			a[0] = -7; a[1] = -9; a[2] = -5; a[3] = -11;
			a[4] = -3; a[5] = -13; a[6] = -1; a[7] = -15;

			b[0] = -8; b[1] = -6; b[2] = -14; b[3] = -4;
			b[4] = -12; b[5] = -2; b[6] = -10; b[7] = 0;

			merge_sort(a, b, 8, 8, tmp);
			print_array(tmp, 16);

			a = realloc(a, 9*sizeof(int));
			b = realloc(b, 9*sizeof(int));
			tmp = realloc(tmp, 18*sizeof(int));
			a[0] = 0; a[1] = 2; a[2] = 4;
			a[3] = 10; a[4] = 8; a[5] = 6;
			a[6] = 12; a[7] = 14; a[8] = 16;

			b[0] = 3; b[1] = 1; b[2] = 5;
			b[3] = 7; b[4] = 9; b[5] = 13;
			b[6] = 17; b[7] = 15; b[8] = 11;

			merge_sort(a, b, 9, 9, tmp);
			print_array(tmp, 18);

			free(a);
			free(b);
			free(tmp);
			return 1;
		}

		switch (argc) {
			case 1:
				seed = time(NULL);
				arr_size = NELEMENTS;
				max_val = MAXVAL;
				break;
			case 2:
				seed = atoi(argv[1]);
				arr_size = NELEMENTS;
				max_val = MAXVAL;
				break;
			case 3:
				seed = atoi(argv[1]);
				arr_size = atoi(argv[2]);
				max_val = MAXVAL;
				break;
			case 4:
				seed = atoi(argv[1]);
				arr_size = atoi(argv[2]);
				max_val = atoi(argv[3]);
				break;
			default:
				printf("Too many arguments\n");
				break;
		}

		// Array initialization and population
		srand(seed);
		sortable = (int*)malloc(arr_size*sizeof(int));
		tmp 	 = (int*)malloc(arr_size*sizeof(int));

		populate_array(sortable, arr_size, max_val);
		memcpy(tmp, sortable, arr_size*sizeof(int));

		print_array(sortable, arr_size);

		/***	TAGS USED FOR COMMUNICATION
			0 - Array size to be sorted
			1 - Passing the array to be ordered for a child process
			2 - Return of the ordered array from a child process to the parent
		***/

		// DIVISION
		int son_rank = get_son_rank(rank, tree_level);
		while (son_rank < comm_world_size) {  // While still possible to divide work
			tree_level++;  // Updating the level in the tree that the process is

			divide_array(arr_size, &a_size, &b_size);
			a = malloc(a_size * sizeof(int));
			b = malloc(b_size * sizeof(int));

			split_array(tmp, a, b, a_size, b_size);

			aux[0] = b_size;
			MPI_Send(&aux, 1, MPI_INT, son_rank, 0, MPI_COMM_WORLD); // Size of array to be sorted
			MPI_Send(b, b_size, MPI_INT, son_rank, 1, MPI_COMM_WORLD);  // The array to be sorted

			arr_size = a_size;
			tmp = realloc(tmp, arr_size * sizeof(int));
			memcpy(tmp, a, arr_size * sizeof(int));

			free(a);
			free(b);

			son_rank = get_son_rank(rank, tree_level);
		}

		// Sequencial merge sort of the rest
		divide_array(arr_size, &a_size, &b_size);
		a = malloc(a_size*sizeof(int));
		b = malloc(b_size*sizeof(int));

		split_array(tmp, a, b, a_size, b_size);
		merge_sort(a, b, a_size, b_size, tmp);

		// CONQUER
		while (tree_level > 0) {
			tree_level--;
			son_rank = get_son_rank(rank, tree_level);

			a_size = arr_size;
			a = realloc(a, a_size * sizeof(int));
			memcpy(a, tmp, a_size * sizeof(int));

			// Receive array size
			MPI_Recv(&aux, 1, MPI_INT, son_rank, 0, MPI_COMM_WORLD, &st);
			b_size = aux[0];

			b = realloc(b, b_size * sizeof(int));
			MPI_Recv(b, b_size, MPI_INT, son_rank, 2, MPI_COMM_WORLD, &st); // Receive array to be sorted

			arr_size = a_size + b_size;
			tmp = realloc(tmp, arr_size * sizeof(int));

			merge_sorted_arrays(a, b, a_size, b_size, tmp);
		}

		free(a);
		free(b);
		free(sortable);

		print_array(tmp, arr_size);

	} else {
		// child processes
		MPI_Recv(&aux, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &st);
		arr_size = aux[0];
		int father_rank = st.MPI_SOURCE;
		tmp = (int*)malloc(arr_size * sizeof(int));

		MPI_Recv(tmp, arr_size, MPI_INT, father_rank, 1, MPI_COMM_WORLD, &st);

		// DIVISION
		int son_rank = get_son_rank(rank, tree_level);

		while (son_rank < comm_world_size) { 
			tree_level++;

		
			divide_array(arr_size, &a_size, &b_size);
			a = (int*)malloc(a_size*sizeof(int));
			b = (int*)malloc(b_size*sizeof(int));

			split_array(tmp, a, b, a_size, b_size);

			aux[0] = b_size;
			MPI_Send(&aux, 1, MPI_INT, son_rank, 0, MPI_COMM_WORLD);
			MPI_Send(b, b_size, MPI_INT, son_rank, 1, MPI_COMM_WORLD);  

			arr_size = a_size;
			tmp = realloc(tmp, arr_size * sizeof(int));
			memcpy(tmp, a, arr_size * sizeof(int));

			free(a);
			free(b);

			son_rank = get_son_rank(rank, tree_level);
		}

		divide_array(arr_size, &a_size, &b_size);
		a = malloc(a_size*sizeof(int));
		b = malloc(b_size*sizeof(int));

		split_array(tmp, a, b, a_size, b_size);

		merge_sort(a, b, a_size, b_size, tmp); 

		// CONQUER
		int initial_level = get_initial_level(rank);
		while (tree_level > initial_level) {
			tree_level--;
			son_rank = get_son_rank(rank, tree_level);

			a_size = arr_size;
			a = realloc(a, a_size * sizeof(int));
			memcpy(a, tmp, a_size * sizeof(int));

			MPI_Recv(&aux, 1, MPI_INT, son_rank, 0, MPI_COMM_WORLD, &st);
			b_size = aux[0];
			b = realloc(b, b_size * sizeof(int));

			MPI_Recv(b, b_size, MPI_INT, son_rank, 2, MPI_COMM_WORLD, &st);

			arr_size = a_size + b_size;
			tmp = realloc(tmp, arr_size * sizeof(int));

			merge_sorted_arrays(a, b, a_size, b_size, tmp);
		}

		free(a);
		free(b);

		// Send message informing the size of the sorted array
		aux[0] = arr_size;
		MPI_Send(&aux, 1, MPI_INT, father_rank, 0, MPI_COMM_WORLD);

		// Send sorted array
		MPI_Send(tmp, arr_size, MPI_INT, father_rank, 2, MPI_COMM_WORLD);
	}

	free(tmp);

	MPI_Finalize();
	return 0;
}
