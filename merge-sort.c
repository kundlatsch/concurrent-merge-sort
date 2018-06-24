// V. Freitas [2018] @ ECL-UFSC
#include <stdlib.h>
#include <time.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <stdarg.h>
#include <mpi.h>

/***
* Todas as Macros pré-definidas devem ser recebidas como parâmetros de
* execução da sua implementação paralela!!
***/

#ifndef DEBUG
#define DEBUG 0
#endif

#ifndef NELEMENTS
#define NELEMENTS 100
#endif

#ifndef MAXVAL
#define MAXVAL 255
#endif // MAX_VAL

/*
* More info on: http://en.cppreference.com/w/c/language/variadic
*/

void debug(const char* msg, ...) {
	if (DEBUG > 2) {
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
	if (end - begin < 2) {
		return;
	} else {
		int middle = (begin + end)/2;
		recursive_merge_sort(numbers, begin, middle, tmp);
		recursive_merge_sort(numbers, middle, end, tmp);
		merge(tmp, begin, middle, end, numbers);
	}
}


// Merge especial, utilizado para mesclar dois arrays distintos (o original trabalha sobre um unico array)
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

// First Merge Sort call
void merge_sort(int* a, int* b, int a_size, int b_size, int* tmp) {
	recursive_merge_sort(a, 0, a_size, tmp);
	memcpy(a, tmp, a_size * sizeof(int));

	recursive_merge_sort(b, 0, b_size, tmp);
	memcpy(b, tmp, b_size * sizeof(int));

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

// Divide o tamanho do array em duas partes
void divide_array(int arr_size, int* a_size, int* b_size) {
	if (arr_size % 2 == 0) {
		*a_size = arr_size/2;
		*b_size = *a_size;
	} else {
		*a_size = (int)arr_size/2;
		*b_size = *a_size + 1;  // Em entradas impares, b fica com o elemento extra
	}
}

// Divide um array em duas partes
void split_array(int* array, int* a, int* b, int a_size, int b_size) {
	for (int i = 0; i < a_size; i++) {
		a[i] = array[i];
	}

	for (int i = 0; i < b_size; i++) {
		b[i] = array[i + a_size];
	}
}

// Retorna o nivel inicial de um processo na arvore de trabalho baseado no rank
int get_initial_level(int rank) {
	if (rank == 0)
		return 0;
	else
		return ((int) log2((double) rank)) + 1;
}

// Retorna o rank do processo filho baseado no rank do processo atual e o nivel que este se encontra na arvore de trabalho
int get_son_rank(int rank, int tree_level) {
	return rank + pow(2, (double) tree_level);
}

int main (int argc, char ** argv) {
	// Variaveis MPI
	int comm_world_size, rank;
	MPI_Status st;

	// Variaveis locais a cada processo
	int tree_level, father_rank;
	size_t arr_size;  // tamanho do vetor tmp
	int *a, *b, a_size, b_size, *tmp, aux[1];  // a, b: arrays que guardam as metades do array tmp

	// Inicializacao MPI
	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &comm_world_size);

	tree_level = get_initial_level(rank);  // Nivel inicial em que um processo se encontra na arvore de trabalho

	// Codigo processo principal
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

			free(a);
			free(b);
			free(tmp);

			a = (int*)malloc(9*sizeof(int));
			b = (int*)malloc(9*sizeof(int));
			tmp = (int*)malloc(18*sizeof(int));
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
			printf("\n");
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

		// Inicializacao e populacao do array
		srand(seed);
		sortable = (int*)malloc(arr_size*sizeof(int));
		tmp 	 = (int*)malloc(arr_size*sizeof(int));

		populate_array(sortable, arr_size, max_val);
		memcpy(tmp, sortable, arr_size*sizeof(int));

		print_array(sortable, arr_size);

		/***	RESUMO TAGS UTILIZADAS PARA COMUNICACAO
			0 - Passagem do tamanho do array ordenado/a ser ordenado
			1 - Passagem do array a ser ordenado para um processo filho
			2 - Retorno do array ordenado de um processo filho ao pai
		***/

		// DIVISAO
		int son_rank = get_son_rank(rank, tree_level);
		while (son_rank < comm_world_size) {  // Ainda e possivel dividir o trabalho
			tree_level++;  // Atualizacao do nivel na arvore que o processo se encontra

			// Divisão dos array a ser ordenado em duas partes
			divide_array(arr_size, &a_size, &b_size);
			a = malloc(a_size * sizeof(int));
			b = malloc(b_size * sizeof(int));

			split_array(tmp, a, b, a_size, b_size);

			// Envia uma das metades para um processo filho
			aux[0] = b_size;
			MPI_Send(&aux, 1, MPI_INT, son_rank, 0, MPI_COMM_WORLD); // Tamanho do array a ser ordenado pelo processo filho
			MPI_Send(b, b_size, MPI_INT, son_rank, 1, MPI_COMM_WORLD);  // Metade do array a ser ordenada

			// Atualizacao de tmp e seu tamanho
			arr_size = a_size;
			tmp = realloc(tmp, arr_size * sizeof(int));
			memcpy(tmp, a, arr_size * sizeof(int));

			free(a);
			free(b);

			son_rank = get_son_rank(rank, tree_level);
		}

		// MERGE-SORT SEQUENCIAL DA PARTE RESTANTE
		divide_array(arr_size, &a_size, &b_size);
		a = malloc(a_size*sizeof(int));
		b = malloc(b_size*sizeof(int));

		split_array(tmp, a, b, a_size, b_size);
		merge_sort(a, b, a_size, b_size, tmp);  // Merge-sort sequencial da parte restante

		// CONQUISTA
		while (tree_level > 0) {
			tree_level--;
			son_rank = get_son_rank(rank, tree_level);

			a_size = arr_size;
			a = realloc(a, a_size * sizeof(int));
			memcpy(a, tmp, a_size * sizeof(int));

			// Recebe o tamanho do array ordenado pelo processo filho para alocar o espaço correto para este
			MPI_Recv(&aux, 1, MPI_INT, son_rank, 0, MPI_COMM_WORLD, &st);
			b_size = aux[0];

			b = realloc(b, b_size * sizeof(int));
			MPI_Recv(b, b_size, MPI_INT, son_rank, 2, MPI_COMM_WORLD, &st); // Recebe a metade do array ordenada pelo processo filho

			// Realoca tmp com o tamanho correto
			arr_size = a_size + b_size;
			tmp = realloc(tmp, arr_size * sizeof(int));

			// Faz o merge das metades ordenadas
			merge_sorted_arrays(a, b, a_size, b_size, tmp);
		}

		free(a);
		free(b);
		free(sortable);

		// Imprime o array ordenado na tela
		print_array(tmp, arr_size);

	} else {
		// CODIGO PROCESSOS FILHOS
		// Recebe o tamanho do array a ser ordenado e aloca os recursos necessarios
		MPI_Recv(&aux, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &st);
		arr_size = aux[0];
		int father_rank = st.MPI_SOURCE;
		tmp = (int*)malloc(arr_size * sizeof(int));

		// Recebe o array a ser ordenado
		MPI_Recv(tmp, arr_size, MPI_INT, father_rank, 1, MPI_COMM_WORLD, &st);

		// DIVISAO
		int son_rank = get_son_rank(rank, tree_level);

		while (son_rank < comm_world_size) {  // Ainda e possivel dividir o trabalho
			tree_level++;  // Atualizacao do nivel na arvore que o processo se encontra

			// Divisão dos array a ser ordenado em duas partes
			divide_array(arr_size, &a_size, &b_size);
			a = (int*)malloc(a_size*sizeof(int));
			b = (int*)malloc(b_size*sizeof(int));

			split_array(tmp, a, b, a_size, b_size);

			// Envia uma das metades para um processo filho
			aux[0] = b_size;
			MPI_Send(&aux, 1, MPI_INT, son_rank, 0, MPI_COMM_WORLD); // Tamanho do array a ser ordenado pelo processo filho
			MPI_Send(b, b_size, MPI_INT, son_rank, 1, MPI_COMM_WORLD);  // Metade do array a ser ordenada

			// Atualizacao de tmp e seu tamanho
			arr_size = a_size;
			tmp = realloc(tmp, arr_size * sizeof(int));
			memcpy(tmp, a, arr_size * sizeof(int));

			free(a);
			free(b);

			son_rank = get_son_rank(rank, tree_level);
		}

		// MERGE-SORT SEQUENCIAL DA PARTE RESTANTE
		divide_array(arr_size, &a_size, &b_size);
		a = malloc(a_size*sizeof(int));
		b = malloc(b_size*sizeof(int));

		split_array(tmp, a, b, a_size, b_size);

		merge_sort(a, b, a_size, b_size, tmp);  // Merge-sort sequencial da parte restante

		// CONQUISTA
		int initial_level = get_initial_level(rank);
		while (tree_level > initial_level) {
			tree_level--;
			son_rank = get_son_rank(rank, tree_level);

			a_size = arr_size;
			a = realloc(a, a_size * sizeof(int));
			memcpy(a, tmp, a_size * sizeof(int));

			// Recebe o tamanho do array ordenado pelo processo filho para alocar o espaço correto para este
			MPI_Recv(&aux, 1, MPI_INT, son_rank, 0, MPI_COMM_WORLD, &st);
			b_size = aux[0];
			b = realloc(b, b_size * sizeof(int));

			MPI_Recv(b, b_size, MPI_INT, son_rank, 2, MPI_COMM_WORLD, &st); // Recebe a metade do array ordenada pelo processo filho

			// Realoca tmp com o tamanho correto
			arr_size = a_size + b_size;
			tmp = realloc(tmp, arr_size * sizeof(int));

			// Faz o merge das metades ordenadas
			merge_sorted_arrays(a, b, a_size, b_size, tmp);
		}

		free(a);
		free(b);

		// Envia uma mensagem informando o tamanho do array ordenado
		aux[0] = arr_size;
		MPI_Send(&aux, 1, MPI_INT, father_rank, 0, MPI_COMM_WORLD);

		// Envia sua parte ordenada de volta ao processo da qual recebeu
		MPI_Send(tmp, arr_size, MPI_INT, father_rank, 2, MPI_COMM_WORLD);
	}

	free(tmp);

	MPI_Finalize();
	return 0;
}
