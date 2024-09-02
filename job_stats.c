#include "job_stats.h"


// FORMAT: User|Group|JobID|ReqTRES|Timelimit|Submit|NodeList|Start|End|Elapsed|State|ExitCode
//	- ignore rows where user is blank

void insert_job_to_db(sqlite3 * jobs_db, Job * job){
	char * insert_statement = "INSERT INTO Jobs "
                        "(job_id, user_name, group_name, n_nodes, n_cpus, n_gpus, mem_mb, billing, time_limit, submit_time, node_list, start_time, end_time, elapsed_time, state, exit_code)";

    char * full_insert_statement;

	asprintf(&full_insert_statement, "%s VALUES (%ld, '%s', '%s', %d, %d, %d, %d, %d, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');", insert_statement, 
										job -> job_id,
										job -> user,
										job -> group,
										job -> n_nodes,
										job -> n_cpus,
										job -> n_gpus,
										job -> mem_mb,
										job -> billing,
										job -> time_limit,
										job -> submit_time,
										job -> node_list,
										job -> start_time,
										job -> end_time,
										job -> elapsed_time,
										job -> state,
										job -> exit_code);

	char *sqlErr;

	int sql_ret = sqlite3_exec(jobs_db, full_insert_statement, NULL, NULL, &sqlErr);
	

	if (sql_ret != SQLITE_OK){
		fprintf(stderr, "SQL error: %s\n", sqlErr);
		fprintf(stdout, "Bad sql line: %s\n", full_insert_statement);
		sqlite3_free(sqlErr);
	}

	free(full_insert_statement);

	return;
}


// Either of the format: billing=96,cpu=96,mem=393216M,node=3
// Or of the format: billing=20,cpu=16,gres/gpu=16,mem=262144M,node=4

int parse_req_tres(Job * job){

	char * req_tres = job -> req_tres;
	int len = strlen(req_tres);
	int comma_cnt = 0;
	for (int i = 0; i < len; i++){
		if (req_tres[i] == ','){
			comma_cnt += 1;
		}
	}
	int billing = 0;
	int n_nodes = 0;
	int n_cpus = 0;
	int n_gpus = 0;
	int mem_mb = 0;
	if (comma_cnt == 3){
		sscanf(req_tres, "billing=%d,cpu=%d,mem=%dM,node=%d", &billing, &n_cpus, &mem_mb, &n_nodes);
	}
	if (comma_cnt == 4){
		sscanf(req_tres, "billing=%d,cpu=%d,gres/gpu=%d,mem=%dM,node=%d", &billing, &n_cpus, &n_gpus, &mem_mb, &n_nodes);
	}

	job -> billing = billing;
	job -> n_nodes = n_nodes;
	job -> n_cpus = n_cpus;
	job -> n_gpus = n_gpus;
	job -> mem_mb = mem_mb;
	
	return 0;

}

void scan_line(char *line, Job * job)
{	

	// occurs when there is no user and shows sub-jobs
	if (line[0] == '|'){
		job -> user[0] = '\0';
		return;
	}

    char * tok = strtok(line, "|");
    strcpy(job -> user, tok);
    tok = strtok(NULL, "|");
    strcpy(job -> group, tok);
    tok = strtok(NULL, "|");
    job -> job_id = atol(tok);

    tok = strtok(NULL, "|");
    strcpy(job -> req_tres, tok);
    parse_req_tres(job);

    tok = strtok(NULL, "|");
    strcpy(job -> time_limit, tok);
    tok = strtok(NULL, "|");
    strcpy(job -> submit_time, tok);
    tok = strtok(NULL, "|");
    strcpy(job -> node_list, tok);
    tok = strtok(NULL, "|");
    strcpy(job -> start_time, tok);
    tok = strtok(NULL, "|");
    strcpy(job -> end_time, tok);
    tok = strtok(NULL, "|");
    strcpy(job -> elapsed_time, tok);
    tok = strtok(NULL, "|");
    strcpy(job -> state, tok);
    tok = strtok(NULL, "|");
    strcpy(job -> exit_code, tok);
}



void dump_sacct_file(sqlite3 * jobs_db, char * outfile){

	FILE * sacct_file = fopen(outfile, "r");

	if (sacct_file == NULL){
		fprintf(stderr, "Error reading sacct output file\n");
		exit(1);
	}


	// one (or zero) jobs per line in file
	Job * job  = malloc(sizeof(Job));

	if (job == NULL){
		fprintf(stderr, "Error malloc for job\n");
		exit(1);
	}

	char buffer[1024];

	// EXPLICITY START DB TRANSACTION SO IT DOESN't AUTO COMMIT
	sqlite3_exec(jobs_db, "BEGIN", 0, 0, 0);


	while(fgets(buffer, sizeof(buffer), sacct_file) != NULL){
			
		scan_line(buffer, job);

		// error reading line
		if (job -> user[0] == '\0'){
			continue;
		}

		insert_job_to_db(jobs_db, job);

	}

	// EXPLICITY COMMIT TRANSACTION
	sqlite3_exec(jobs_db, "COMMIT", 0, 0, 0);

	free(job);
	fclose(sacct_file);

}

void collect_job_stats(sqlite3 * jobs_db, char * out_dir, char * hostname, long time_sec){

	char * cmd;
	asprintf(&cmd, "sacct --nodelist=%s --format=User,Group,JobID,ReqTRES,Timelimit,Submit,NodeList,Start,End,Elapsed,State,ExitCode --state=COMPLETED,CANCELLED,FAILED,TIMEOUT,OUT_OF_MEMORY --starttime=now-1hour --endtime=now --unit=M --allusers -P -n", hostname);

	char * outfile;
	asprintf(&outfile, "%s/%s_temp.txt", out_dir, hostname);

	char * full_cmd;

	asprintf(&full_cmd, "%s > %s", cmd, outfile);

	// CALL SACCT
	system(full_cmd);

	dump_sacct_file(jobs_db, outfile);

	free(cmd);
	free(full_cmd);
	
	// remove temp file
	if (remove(outfile) != 0){
		fprintf(stderr, "Could not remove temp file\n");
	}
	
	free(outfile);
}
