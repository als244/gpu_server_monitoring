#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <time.h>
#include <getopt.h>
#include <sys/sysinfo.h>
#include <sys/types.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <math.h>

#include <sqlite3.h>


void collect_job_stats(sqlite3 * job_db, char * out_dir, char * hostname, long time_sec);


typedef struct job
{
	char user[21];
	char group[21];
	long job_id;
	char req_tres[255];
	char time_limit[9];
	char submit_time[20];
	char node_list[255];
	char start_time[20];
	char end_time[20];
	char elapsed_time[9];
	char state[21];
	char exit_code[4];
	int n_nodes;
	int n_cpus;
	int n_gpus;
	int mem_mb;
	int billing;
} Job;
