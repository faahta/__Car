#define UNICODE
#define _UNICODE
#define _CRT_SECURE_NO_WARNINGS

#include <windows.h>
#include <tchar.h>
#include <process.h>
#include <stdio.h>
#include<stdlib.h>
#include<stdint.h>
#include<stdbool.h>

#define MAX_NAME_LENGTH 40
#define QUEUE_CAPACITY 100

uint32_t N;
TCHAR inputFile[MAX_PATH + 1], outputFile[MAX_PATH + 1];
int done;

typedef struct car {
	TCHAR manName[MAX_NAME_LENGTH];
	TCHAR carName[MAX_NAME_LENGTH];
	TCHAR tradeFile[MAX_PATH + 1];
}Car;
typedef struct trade {
	TCHAR branch[MAX_NAME_LENGTH];
	TCHAR saleDate[MAX_NAME_LENGTH];
	uint32_t price;
}Trade;
typedef struct queue {
	Trade* trades;
	int* front, * rear;
	int size, capacity;
}Queue;
typedef struct stat {
	int id;	/*thread id*/
	Car* info;
	TCHAR branch[MAX_NAME_LENGTH]; /*branch that sold most cars*/
	int num_cars_sold;
	int total_sale;
	TCHAR date_most_sold[MAX_NAME_LENGTH];
}Stat;

typedef struct counter { /*To Identify the branch who sold most and date*/
	LPTSTR name;
	int count;
}Counter;

typedef struct thread {
	int id;
	FILE* fpIn, * fpOut;
	CRITICAL_SECTION cs1;/*for input*/
	CRITICAL_SECTION cs2;/*for output*/
}threads_t;


threads_t *threadData;

Queue* initQueue();
void enqueueTrade(Queue*, Trade);
Trade dequeueTrade(Queue*);

bool readCar(FILE*, Trade*);
bool readTrade(FILE*, Trade*);
void CalculateStat(Queue*, Stat*, Counter *);
void writeStat(FILE *, Stat );

DWORD WINAPI threadFunction(threads_t);

int _tmain(int argc, _TCHAR** argv) {

	FILE* fpIn, * fpOut;
	HANDLE* hThreads;
	int i;

	_stscanf_s(argv[1], _T("%s"), inputFile, MAX_PATH + 1);
	N = atoi(argv[2]);
	_stscanf_s(argv[3], _T("%s"), outputFile, MAX_PATH + 1);
	
	

	if (_tfopen_s(&fpIn, inputFile, _T("r")) == 0) {
		if (_tfopen_s(&fpOut, outputFile, _T("w")) == 0) {
			/*create the threads*/
			hThreads = (HANDLE*)malloc(N * sizeof(HANDLE));
			threadData = (threads_t*)malloc(N * sizeof(threads_t));
			for (i = 0; i < N; i++) {
				threadData[i].id = i;
				threadData[i].fpIn = &fpIn;
				threadData[i].fpOut = &fpOut;
				InitializeCriticalSection(&threadData[i].cs1);
				InitializeCriticalSection(&threadData[i].cs2);
				hThreads[i] = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)threadFunction, &threadData[i],0, NULL);
				if (hThreads[i] == NULL) {
					_tprintf(_T("invalid handle\n"));
					ExitProcess(0);
				}
			}
		}
	}
	WaitForMultipleObjects(N, hThreads, TRUE, INFINITE);
	for (i = 0; i < N; i++) {
		CloseHandle(hThreads[i]);
	}


	return 0;
}

DWORD WINAPI threadFunction(threads_t data) {

	done = 0;
	while (!done) {
		Car* car;
		EnterCriticalSection(&data.cs1);
		if (!readCar(&data.fpIn, &car)) { done = 1; }
		LeaveCriticalSection(&data.cs1);
		if (!done) {
			/*open trade file*/
			FILE* fpTr;
			if (_tfopen_s(&fpTr, &car->tradeFile, _T("r"))) {
				/*read trade and enqueue sale data*/
				Stat stat;
				Trade* trade = (Trade*)malloc(sizeof(Trade));
				Queue* queue = initQueue();
				while (readTrade(&fpTr, &trade)) {
					enqueueTrade(queue, *trade);
					Trade* trade = (Trade*)malloc(sizeof(Trade));
				}
				free(trade);
				stat.info = &car;
				stat.id = data.id;
				Counter* counter = (Counter*)malloc(queue->size * sizeof(Counter));
				CalculateStat(queue, &stat, &counter);
			}
			else { _tprintf(_T("Can not open trade file %s\n"), car->tradeFile); return 2; }

			fclose(fpTr);

			EnterCriticalSection(&data.cs2);
			//write the stat
			writeStat(&data.fpOut, &stat);
			LeaveCriticalSection(&data.cs2);
		}
	}
	ExitThread(0);
}

bool readCar(FILE *fp, Car *car) {
	bool res = fread(&car, sizeof(car), 1, fp);
	return res;
}
bool readTrade(FILE *fp, Trade *trade) {
	bool res = fread(&trade, sizeof(trade), 1, fp);
	return res;
}

Queue * initQueue() {
	Queue *queue = (Queue*)malloc(sizeof(Queue));
	queue->trades = (Trade*)malloc(QUEUE_CAPACITY * sizeof(Trade));
	queue->front = queue->size = 0;
	queue->capacity = QUEUE_CAPACITY;
	queue->rear = queue->capacity - 1;
	return queue;
}

bool isFull(Queue* q) { return (q->size == q->capacity); }
bool isEmpty(Queue* q) { return (q->size == 0); }

void enqueueTrade(Queue* q, Trade trade) {
	if (isFull(q))
		return;
	q->rear = (int)(q->rear + 1) % QUEUE_CAPACITY;
	q->trades[(int)q->rear] = trade;
	q->size += 1;
}
Trade dequeueTrade(Queue *q) {
	Trade tr;
	if (isEmpty(q))
		return;
	tr = q->trades[(int)q->front];
	q->front = (int)(q->front + 1) % QUEUE_CAPACITY;
	q->size -= 1;
	return tr;
}


void CalculateStat(Queue *q, Stat *stat, Counter *counter) {
	/*Number of cars sold*/
	stat->num_cars_sold = q->size;
	/*Total price amount sold*/
	int i, tot_sold = 0;
	for (i = 0; i < q->size; i++) {
		tot_sold += q->trades[i].price;
	}
	stat->total_sale = tot_sold;

	/*The branch who sold many cars*/
	int j,count=0; 
	for (i = 0; i < q->size; i++) {
		int unique = 1;
		for (j = 0; j < i; j++) {
			if (strcmp(q->trades[i].branch,q->trades[j].branch) == 0) {		
				unique = 0;
				count++;
				break;
			}
		}
		if (unique) {
			++counter[i].count;
			counter[i].name = q->trades[i].saleDate;
		}
	}
	int maxCount = counter[0].count;
	LPTSTR branch_most_sold;
	for (i = 0; i < q->size; i++) {
		if (counter[i].count > maxCount) {
			maxCount = counter[i].count;
			branch_most_sold = counter[i].name;
		}
	}
	strcpy(stat->branch, branch_most_sold);
	
	
	/*Data most models sold*/
	for (i = 0; i < q->size; i++) {
		int unique = 1;
		for (j = 0; j < i; j++) {
			if (strcmp(q->trades[i].saleDate, q->trades[j].saleDate) == 0) {
				unique = 0;
				count++;
				break;
			}
		}
		if (unique) {
			++counter[i].count;
			counter[i].name = q->trades[i].saleDate;
		}
	}
	int maxCount = counter[0].count;
	LPTSTR date_most_sold;
	for (i = 0; i < q->size; i++) {
		if (counter[i].count > maxCount) {
			maxCount = counter[i].count;
			branch_most_sold = counter[i].name;
		}
	}
	strcpy(stat->date_most_sold, date_most_sold);

	return;
}

void writeStat(FILE *fp, Stat stat) {
	_ftprintf(fp, _T("Thread ID: %d\n"), stat.id);
	_ftprintf(fp, _T("Car: %s %s\n"),stat.info->carName, stat.info->manName);
	_ftprintf(fp, _T("Units sold: %d, total price: %d\n"),stat.num_cars_sold, stat.total_sale);
	_ftprintf(fp, _T("Branch with most sales: %s\n"), stat.branch);
	_ftprintf(fp, _T("Date with most sales: %s\n"), stat.date_most_sold);
	return;
}