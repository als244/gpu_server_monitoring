#define _GNU_SOURCE

#include "job_stats.h"
#include "dcgm_agent.h"
#include "dcgm_fields.h"
#include "dcgm_structs.h"

#include "monitoring.h"

#define PRINT 0
#define TO_COLLECT_JOB_STATS 1
#define JOBS_STATS_FREQ_SEC 60 * 60
#define DEFAULT_OUTPUT_DIR ""

// CPU MONITORING
Proc_Data * process_proc_stat(Sample * cur_sample, Proc_Data * prev_data){

	FILE * fp = fopen("/proc/stat", "r");

	if (fp == NULL){
		fprintf(stderr, "Error Opening /proc/stat\n");
		return NULL;
	}

	Proc_Data * proc_data = cur_sample -> cpu_util;

	// QUERY MEMORY INFO
	long avail_pages = sysconf(_SC_AVPHYS_PAGES);
	long total_pages = sysconf(_SC_PHYS_PAGES);
	proc_data -> mem_used_pct = 100 * ((double) (total_pages - avail_pages) / (double) total_pages);
	long page_size = sysconf(_SC_PAGESIZE);
	long free_mem_mb = (avail_pages * page_size) / (1 << 20);
	proc_data -> free_mem = free_mem_mb;
	
	// only collecting aggregate 
	Cpu_stat cpu_stats;

	// parse all the cpu data
	char dummy_name[255];
	// only look the top line which aggregates all CPUS
	fscanf(fp, "%s %lu %lu %lu %lu %lu %lu %lu", dummy_name, &(cpu_stats.t_user), &(cpu_stats.t_nice), 
		&(cpu_stats.t_system), &(cpu_stats.t_idle), &(cpu_stats.t_iowait), &(cpu_stats.t_irq),
		&(cpu_stats.t_softirq));

	//done with file, closing
	fclose(fp);
  

	long total_time = cpu_stats.t_user + cpu_stats.t_nice + cpu_stats.t_system + cpu_stats.t_idle + cpu_stats.t_iowait + cpu_stats.t_irq + cpu_stats.t_softirq;
	long idle_time = cpu_stats.t_idle;

	// if there wasn't a previous sample can't compute util %
	if (prev_data == NULL){
		proc_data -> util_pct = 0;
		proc_data -> total_time = total_time;
		proc_data -> idle_time = idle_time;
		return proc_data;
	}

	long prev_total_time = prev_data -> total_time;
	long prev_idle_time = prev_data -> idle_time;


	long total_delta = total_time - prev_total_time;
	long idle_delta = idle_time - prev_idle_time;

	long cpu_used = total_delta - idle_delta;

	double util_pct = (100 * (double) cpu_used) / ((double) total_delta);

	proc_data -> util_pct = util_pct;
	proc_data -> total_time = total_time;
	proc_data -> idle_time = idle_time;

	return proc_data;
}


Net_Data * process_net_stat(Sample * cur_sample, Interface_Totals * interface_totals){

	Net_Data * net_data = cur_sample -> net_util;

	long total_ib_rx_bytes = 0;
	long total_ib_tx_bytes = 0;
	long total_eth_rx_bytes = 0;
	long total_eth_tx_bytes = 0;
	

	FILE * net_stat_fp;
	long cur_rx, cur_tx;
	char * if_path;

	int n_ib_devs = interface_totals -> n_ib_devs;
	int n_eth_ifs = interface_totals -> n_eth_ifs;
	char ** ib_devs = interface_totals -> ib_devs;
	char ** eth_ifs = interface_totals -> eth_ifs;
	
	// ACCUMULATING TOTALS FOR IB IFs

	// ALL PHYS TRAFFIC (including RDMA)
	for (int i = 0; i < n_ib_devs; i++){
		// RX
		// ib_ifs[i] + 2 because we need to get the numerical port for ib device
		asprintf(&if_path, "/sys/class/infiniband/%s/ports/1/counters/port_rcv_data", ib_devs[i]);
		net_stat_fp = fopen(if_path, "r");
		// error couldn't read file
		if (net_stat_fp == NULL){
			fprintf(stderr, "Error: couldn't read rx_bytes at file: %s\n", if_path);
			free(if_path);
			continue;
		}
		fscanf(net_stat_fp, "%ld", &cur_rx);
		// need to multiply by 4 because port_recv_data is divided by 4
		total_ib_rx_bytes += 4 * cur_rx;
		free(if_path);
		fclose(net_stat_fp);

		// TX
		asprintf(&if_path, "/sys/class/infiniband/%s/ports/1/counters/port_xmit_data", ib_devs[i]);
		net_stat_fp = fopen(if_path, "r");
		// error couldn't read file
		if (net_stat_fp == NULL){
			free(if_path);
			continue;
		}
		fscanf(net_stat_fp, "%ld", &cur_tx);
		// need to multiply by 4 because port_xmit_data is divided by 4
		total_ib_tx_bytes += 4 * cur_tx;
		free(if_path);
		fclose(net_stat_fp);
	}
			
	// Eth Interface		
	for (int i = 0; i < n_eth_ifs; i++){
		// RX
		asprintf(&if_path, "/sys/class/net/%s/statistics/rx_bytes", eth_ifs[i]);
		net_stat_fp = fopen(if_path, "r");
		// error couldn't read file
		if (net_stat_fp == NULL){
			free(if_path);
			continue;
		}
		fscanf(net_stat_fp, "%ld", &cur_rx);
		total_eth_rx_bytes += cur_rx;
		free(if_path);
		fclose(net_stat_fp);

		// TX
		asprintf(&if_path, "/sys/class/net/%s/statistics/tx_bytes", eth_ifs[i]);
		net_stat_fp = fopen(if_path, "r");
		// error couldn't read file
		if (net_stat_fp == NULL){
			free(if_path);
			continue;
		}
		fscanf(net_stat_fp, "%ld", &cur_tx);
		total_eth_tx_bytes += cur_tx;
		free(if_path);
		fclose(net_stat_fp);
	}



	// Taking these totals minus prev recorded totals
	net_data -> ib_rx_bytes = total_ib_rx_bytes - interface_totals -> total_ib_rx_bytes;
	net_data -> ib_tx_bytes = total_ib_tx_bytes - interface_totals -> total_ib_tx_bytes;
	net_data -> eth_rx_bytes = total_eth_rx_bytes - interface_totals -> total_eth_rx_bytes;
	net_data -> eth_tx_bytes = total_eth_tx_bytes - interface_totals -> total_eth_tx_bytes;

	// Special case for the first time we don't want to have outlier results
	if (interface_totals -> total_ib_rx_bytes == 0){
		net_data -> ib_rx_bytes = 0;
		net_data -> ib_tx_bytes = 0;
		net_data -> eth_rx_bytes = 0;
		net_data -> eth_tx_bytes = 0;
	}

	// Update new totals
	interface_totals -> total_ib_rx_bytes = total_ib_rx_bytes;
	interface_totals -> total_ib_tx_bytes = total_ib_tx_bytes;
	interface_totals -> total_eth_rx_bytes = total_eth_rx_bytes;
	interface_totals -> total_eth_tx_bytes = total_eth_tx_bytes;

	return net_data;
}

// GPU MONITORING

// < 100 values, do not need hash table
int get_my_field_ind(unsigned short fieldId, unsigned short * field_ids, int n_fields){

	for (int i = 0; i < n_fields; i++){
		if (field_ids[i] == fieldId){
			return i;
		}
	}
	return -1;
}

// pass in memory location of pointer to allocated array
int copy_field_values_function(unsigned int gpuId, dcgmFieldValue_v1 * values, int numValues, void * userdata){
	Samples_Buffer * samples_buffer = (Samples_Buffer *) userdata;
	int n_samples = samples_buffer -> n_samples;
	Sample * cur_sample = &((samples_buffer -> samples)[n_samples]);
	unsigned short * field_ids = samples_buffer -> field_ids;
	int n_fields = samples_buffer -> n_fields;
	// hardcoded but could use the fieldTypes
	int field_size_bytes = 8;
	unsigned short fieldId, fieldType;
	int indOfField;
	for (int i = 0; i < numValues; i++){
		fieldId = values[i].fieldId;
		indOfField = get_my_field_ind(fieldId, field_ids, n_fields);
		if (indOfField == -1){
			continue;
		}
		fieldType = values[i].fieldType;
		if (fieldType == DCGM_FT_DOUBLE){
			memcpy(cur_sample -> field_values + (gpuId * n_fields + indOfField) * field_size_bytes, &(values[i].value.dbl), field_size_bytes);
		}
		else if ((fieldType == DCGM_FT_INT64) || (fieldType == DCGM_FT_TIMESTAMP)){
			memcpy(cur_sample -> field_values + (gpuId * n_fields + indOfField) * field_size_bytes, &(values[i].value.i64), field_size_bytes);
		}
		else{
			// fieldType not supported
			continue;
		}		
	}
	return 0;
}

void insert_sample_to_db(sqlite3 * db, long timestamp_ms, long device_id, long field_id, long value){

	char * insert_statement;

	asprintf(&insert_statement, "INSERT INTO Data (timestamp,device_id,field_id,value) VALUES (%ld, %ld, %ld, %ld);", timestamp_ms, device_id, field_id, value);

	char *sqlErr;

	int sql_ret = sqlite3_exec(db, insert_statement, NULL, NULL, &sqlErr);
	
	free(insert_statement);

	if (sql_ret != SQLITE_OK){
		fprintf(stderr, "SQL error: %s\n", sqlErr);
		sqlite3_free(sqlErr);
	}
	return;
}


int dump_samples_buffer(Samples_Buffer * samples_buffer, sqlite3 * db){

	int n_fields = samples_buffer -> n_fields;
	int n_devices = samples_buffer -> n_devices;

	// hardcoded, but could also look at field_types field in sample struct
	int field_size_bytes = 8;

	int n_samples = samples_buffer -> n_samples;
	unsigned short * fieldIds = samples_buffer -> field_ids;
	unsigned short * fieldTypes = samples_buffer -> field_types;

	Sample * samples = samples_buffer -> samples;

	// Saving Data
	Proc_Data * cpu_data;
	Net_Data * net_data;
	void * fieldValues;
	Sample data;

	unsigned short fieldId, fieldType;
	long ind, time_ns;

	long val;
	// insert timestamp and field values for every sample
	struct timespec start, end;
	clock_gettime(CLOCK_REALTIME, &start);

	// EXPLICITY START DB TRANSACTION SO IT DOESN't AUTO COMMIT
	sqlite3_exec(db, "BEGIN", 0, 0, 0);	
	

	for (int i = 0; i < n_samples; i++){

		data = samples[i];
		time_ns = data.time.tv_sec * 1e9 + data.time.tv_nsec;

		// CPU dump
		cpu_data = data.cpu_util;
		// HARDCODING FIELDS:
		//	- 0 = free_mem
		//	- 1 = util_pct
		insert_sample_to_db(db, time_ns, -1, 1, round(cpu_data -> mem_used_pct));
		insert_sample_to_db(db, time_ns, -1, 2, cpu_data -> free_mem);
		insert_sample_to_db(db, time_ns, -1, 3, round(cpu_data -> util_pct));

		// NET dump
		net_data = data.net_util;
		// HARDCODING FIELDS:
		//	- 2 = ib_rx_bytes
		//	- 3 = ib_tx_bytes
		//	- 4 = eth_rx_bytes
		//	- 5 = eth_tx_bytes
		insert_sample_to_db(db, time_ns, -1, 10, net_data -> ib_rx_bytes);
		insert_sample_to_db(db, time_ns, -1, 11, net_data -> ib_tx_bytes);
		// SAVE DB SPACE BY NOT STORING ETH DATA. 
		// PRETTY MUCH NEVER USED SO MIGHT WANT TO COMMENT OUT
		//insert_sample_to_db(db, time_ns, -1, 14, net_data -> eth_rx_bytes);
		//insert_sample_to_db(db, time_ns, -1, 15, net_data -> eth_tx_bytes);
		
		// GPU Field dump
		fieldValues = data.field_values;

		for (int gpuId = 0; gpuId < n_devices; gpuId++){
			for (int fieldNum = 0; fieldNum < n_fields; fieldNum++){
				ind = gpuId * n_fields + fieldNum;
				fieldId = fieldIds[fieldNum];
				fieldType = fieldTypes[fieldNum];
				switch (fieldType) {
					case DCGM_FT_DOUBLE:
						// all the doubles are fractions 0-1, we instead represent as int 0-100
						val = (long) round(((double *) fieldValues)[ind] * 100);
						break;
					case DCGM_FT_INT64:
						val =  (((long *) fieldValues)[ind]);
						break;
					case DCGM_FT_TIMESTAMP:
						val = (((long *) fieldValues)[ind]);
						break;
					default:
						val = 0;
						break;
				}
				insert_sample_to_db(db, time_ns, gpuId, fieldId, val);
			}
		}
	}
	
	// EXPLICITY COMMIT TRANSACTION
	sqlite3_exec(db, "COMMIT", 0, 0, 0);

	clock_gettime(CLOCK_REALTIME, &end);

	long elapsed_time_ns = ((end.tv_sec - start.tv_sec) * 1e9) + (end.tv_nsec - start.tv_nsec);
	long elapsed_time_ms = elapsed_time_ns / 1e6;
	//printf("Elasped time of dump: %lu ms\n", elapsed_time_ms);
	//fflush(stdout);


	// reset samples
	struct timespec time;
	for (int i = 0; i < n_samples; i++){
		samples[i].time = time;
		memset(samples[i].field_values, 0, n_fields * n_devices * field_size_bytes);
	}

	return 0;
	
}

void cleanup_and_exit(int error_code, dcgmHandle_t * dcgmHandle, dcgmGpuGrp_t * groupId, dcgmFieldGrp_t * fieldGroupId){

	// if cleanup was caused by error
	if ((error_code != -1) && (error_code != DCGM_ST_OK)){
		printf("ERROR: %s\nFreeing Structs And Exiting...\n", errorString(error_code));
	}

	if (fieldGroupId){
		dcgmFieldGroupDestroy(*dcgmHandle, *fieldGroupId);
	}

	if (groupId){
		dcgmGroupDestroy(*dcgmHandle, *groupId);
	}

	if (dcgmHandle){
		dcgmStopEmbedded(*dcgmHandle);
	}

	dcgmShutdown();

	exit(error_code);

}

Interface_Totals * init_interface_totals(){
	Interface_Totals * interface_totals = (Interface_Totals *) malloc(sizeof(Interface_Totals));
	if (interface_totals == NULL){
		fprintf(stderr, "Could not allocate memory for interface names\n");
		return NULL;
	}

	const char * eth_interface_parent_dir = "/sys/class/net";
	DIR *dr = opendir(eth_interface_parent_dir);
	if (dr == NULL) { 
		fprintf(stderr, "Could not open interface directory\n"); 
		return NULL;
	}

	struct dirent * eth_interface_dirs;
	int max_ifs = 16;
	char ** ib_devs = (char **) malloc(max_ifs * sizeof(char *));
	int n_ib_devs = 0;
	char ** eth_ifs = (char **) malloc(max_ifs * sizeof(char *));
	int n_eth_ifs = 0;
	
	char * dir_name; 
	while ((eth_interface_dirs = readdir(dr)) != NULL) {
		dir_name = eth_interface_dirs -> d_name;
		if (strncmp("eno", dir_name, 3) == 0){
			eth_ifs[n_eth_ifs] = strdup(dir_name);
			n_eth_ifs++;
		}
	}

	closedir(dr);

	const char * ib_class_parent_dir = "/sys/class/infiniband";
	dr = opendir(ib_class_parent_dir);

	struct dirent * ib_interface_dirs;
	while ((ib_interface_dirs = readdir(dr)) != NULL) {
			dir_name = ib_interface_dirs -> d_name;
			if (strncmp("mlx5", dir_name, 4) == 0){
				ib_devs[n_ib_devs] = strdup(dir_name);
				n_ib_devs++;
			}
	}
	closedir(dr);

	interface_totals -> n_ib_devs = n_ib_devs;
	interface_totals -> ib_devs = ib_devs;
	interface_totals -> n_eth_ifs = n_eth_ifs;
	interface_totals -> eth_ifs = eth_ifs;

	interface_totals -> total_ib_rx_bytes = 0;
	interface_totals -> total_ib_tx_bytes = 0;
	interface_totals -> total_eth_rx_bytes = 0;
	interface_totals -> total_eth_tx_bytes = 0;
	
	return interface_totals;
}


Samples_Buffer * init_samples_buffer(int n_cpu, int clk_tck, int n_devices, int n_fields, unsigned short * field_ids, unsigned short * field_types, int max_samples){

	Samples_Buffer * samples_buffer = (Samples_Buffer *) malloc(sizeof(Samples_Buffer));
	if (samples_buffer == NULL){
		fprintf(stderr, "Could not allocate memory for samples buffer, exiting...\n");
		return NULL;
	}

	samples_buffer -> n_cpu = n_cpu;
	samples_buffer -> clk_tck = clk_tck;
	samples_buffer -> n_devices = n_devices;
	samples_buffer -> n_fields = n_fields;
	samples_buffer -> field_ids = field_ids;
	samples_buffer -> field_types = field_types;
	samples_buffer -> max_samples = max_samples;
	samples_buffer -> n_samples = 0;
	Sample * samples = (Sample *) malloc(max_samples * sizeof(Sample));
	if (samples == NULL){
		fprintf(stderr, "Could not allocate memory for samples buffer, exiting...\n");
		return NULL;
	}

	// hardcoded because only doubles and i64 field value types
	int field_size_bytes = 8;
	for (int i = 0; i < max_samples; i++){
		Sample my_sample;
		my_sample.field_values = (void *) malloc(n_fields * n_devices * field_size_bytes);
		my_sample.cpu_util = (Proc_Data *) malloc(sizeof(Proc_Data));
		my_sample.net_util = (Net_Data *) malloc(sizeof(Net_Data));
		if ((my_sample.field_values == NULL) || (my_sample.cpu_util == NULL) || (my_sample.net_util == NULL)){
			fprintf(stderr, "Could not allocate memory for values in samples buffer, exiting...\n");
			return NULL;
		}
		samples[i] = my_sample;
	}

	samples_buffer -> samples = samples;

	samples_buffer -> interface_totals = init_interface_totals();

	return samples_buffer;

}


void print_usage(){
	const char * usage_str = "Usage: [-f, --fields=<string: comma separated of field ids>] || \
					[-s, --sample_freq_millis=<int>] || \
					[-n, --n_samples_per_buffer=<int: number of samples to hold in-mem before dumping to file>] || \
					[-o, --output_dir=<string: directory to store outputted results]";
	
	printf("%s\n", usage_str);
}

unsigned short * parse_string_to_arr(char * str, int * n_vals){

	char * str_cpy = strdup(str);

	int n_commas = 0;
	int len = strlen(str_cpy);
	for (int i = 0; i < len; i++){
		if (str_cpy[i] == ','){
			n_commas++;
		}
	}

	int size = n_commas + 1;

	*n_vals = size;

	unsigned short * arr = (unsigned short *) malloc(size * sizeof(unsigned short));

	const char * delim = ", ";
	char * token;

	// first token
	token  = strtok(str_cpy, delim);

	int ind = 0;
	while (token != NULL){
		arr[ind] = (unsigned short) atoi(token);
		ind++;
		token = strtok(NULL, delim);
	}

	free(str_cpy);

	return arr; 


}


int main(int argc, char ** argv, char * envp[]){

	// handle command line args

	// deafult args
	int n_fields = 10;
	char * field_ids_string = "203,254,1002,1003,1004,1005,1009,1010,1011,1012";
	int sample_freq_millis = 100;
	int n_samples_per_buffer = 6000;
	// deafult for Della
	// location where the per-host databases are 
	

	char * output_dir = DEFAULT_OUTPUT_DIR;

	

	static struct option long_options[] = {
		{"fields", required_argument, 0, 'f'},
		{"sample_freq_millis", required_argument, 0, 's'},
		{"n_samples_per_buffer", required_argument, 0, 'n'},
		{"output_dir", required_argument, 0, 'o'},
		{0, 0, 0, 0}
	};

	int opt_index = 0;
	int opt;
	while ((opt = getopt_long(argc, argv, "f:s:n:o:", long_options, &opt_index)) != -1){
		switch (opt){
			case 'f': field_ids_string = optarg;
				break;
			case 's': sample_freq_millis = atoi(optarg);
				break;
			case 'n': n_samples_per_buffer = atoi(optarg);
				break;
			case 'o': output_dir = optarg;
				break;
			default: print_usage();
				exit(1);
		}
	}

	// appending hostname to the output directory to store values for this host
	char * hostbuffer = malloc(256 * sizeof(char));
	int hostname_ret = gethostname(hostbuffer, 256);
	if (hostname_ret == -1){
		fprintf(stderr, "Could not get hostname, exiting...\n");
		exit(1);
	}

	//printf("True directory: %s\n", true_output_dir);

	// convert the fieldId comma separted string to array
	unsigned short * fieldIds = parse_string_to_arr(field_ids_string, &n_fields);


	/* DCGM SETUP */
	dcgmReturn_t dcgm_ret; 
	dcgm_ret = dcgmInit();

	if (dcgm_ret != DCGM_ST_OK){
		fprintf(stderr, "INIT ERROR, Exiting...\n");
		cleanup_and_exit(dcgm_ret, NULL, NULL, NULL);
	}

	dcgmHandle_t dcgmHandle;
	// Start embedded process
	dcgm_ret = dcgmStartEmbedded(DCGM_OPERATION_MODE_MANUAL, &dcgmHandle);

	if (dcgm_ret != DCGM_ST_OK){
		fprintf(stderr, "CONNECT ERROR, Exiting...\n");
		cleanup_and_exit(dcgm_ret, &dcgmHandle, NULL, NULL);
	}


	/* READ SYSTEM INFO */

	unsigned int gpuIdList[DCGM_MAX_NUM_DEVICES];
	int n_devices;

	dcgm_ret = dcgmGetAllSupportedDevices(dcgmHandle, gpuIdList, &n_devices);

	if (dcgm_ret != DCGM_ST_OK){
		fprintf(stderr, "GET DEVICES ERROR, Exiting...\n");
		cleanup_and_exit(dcgm_ret, &dcgmHandle, NULL, NULL);
	}	

	// no GPUs in system
	if (n_devices == 0){
		fprintf(stderr, "No GPUs in System, Exiting...\n");
		cleanup_and_exit(dcgm_ret, &dcgmHandle, NULL, NULL);
	}
	//printf("Found %d GPUs\n", n_devices);

	// create group with all devices

	// GROUP_DEFAULT creates group with all entities present on system
	char groupName[] = "MyGroup";
	dcgmGpuGrp_t groupId;
	dcgm_ret = dcgmGroupCreate(dcgmHandle, DCGM_GROUP_DEFAULT, groupName, &groupId);

	if (dcgm_ret != DCGM_ST_OK){
		fprintf(stderr, "GROUP CREATE ERROR, Exiting...\n");
		cleanup_and_exit(dcgm_ret, &dcgmHandle, &groupId, NULL);
	}

	// create field group with all the metrics we want to scan

	dcgmFieldGrp_t fieldGroupId;
	char fieldGroupName[] = "MyFieldGroup";

	/* DEFAULT FIELDS BEING COLLECTED */

	/* 203: COASE GPU UTIL
	 * 254: % Used Frame Buffer
	 * 1002: SM_ACTIVE: Ratio of cycles at least 1 warp assigned to any SM
	 * 1003: SM_OCCUPANCY: Ratio of warps resident to theoretical maximum warps per cycle
	 * 1004: PIPE_TENSOR_ACTIVE: ratio of cycles any tensor pipe is active
	 * 1005: DRAM_ACTIVE: Ratio of cycles device memory interface is sending or receiving data
	 * 1009: PCIe Sent Bytes
	 * 1010: PCIe Recv Bytes
	 * 1011: NVLink Sent Bytes
	 * 1012: NVLink Recv Bytes 
	*/

	// from command line args

	dcgm_ret = dcgmFieldGroupCreate(dcgmHandle, n_fields, fieldIds, fieldGroupName, &fieldGroupId);
	if (dcgm_ret != DCGM_ST_OK){
		fprintf(stderr, "FIELD GROUP CREATE ERROR, Exiting...\n");
		cleanup_and_exit(dcgm_ret, &dcgmHandle, &groupId, &fieldGroupId);
	}
	// watch fields by combining device group and field group

	// update every second
	// sample freq millis from command line
	long long update_freq_micros = sample_freq_millis * 1000;

	// don't cache old metrics for more than 1 sec
	double max_keep_seconds = 1;

	int max_keep_samples = n_samples_per_buffer;

	dcgm_ret = dcgmWatchFields(dcgmHandle, groupId, fieldGroupId, update_freq_micros, max_keep_seconds, max_keep_samples);

	if (dcgm_ret != DCGM_ST_OK){
		fprintf(stderr, "WATCH FIELDS ERROR, Exiting...\n");
		cleanup_and_exit(dcgm_ret, &dcgmHandle, &groupId, &fieldGroupId);
	}

	/* FINISHED INIT SETUP FOR DCGM, NOW INIT THE SAMPLE BUFFER TO STORE VALUES*/
	DcgmFieldsInit();

	unsigned short * fieldTypes = (unsigned short *) malloc(n_fields * sizeof(unsigned short));
	dcgm_field_meta_p meta_ptr;
	for (int i = 0 ; i < n_fields; i++){
		meta_ptr = DcgmFieldGetById(fieldIds[i]);
		if (meta_ptr == NULL){
			fprintf(stderr, "Unknown field %d\n", fieldIds[i]);
			print_usage();
			cleanup_and_exit(dcgm_ret, &dcgmHandle, &groupId, &fieldGroupId);
		}
		fieldTypes[i] = (unsigned short) meta_ptr -> fieldType;
	}
	


	int n_cpu = sysconf(_SC_NPROCESSORS_ONLN);
	int clk_tck = sysconf(_SC_CLK_TCK);

	Samples_Buffer * samples_buffer = init_samples_buffer(n_cpu, clk_tck, n_devices, n_fields, fieldIds, fieldTypes, n_samples_per_buffer);
	if (samples_buffer == NULL){
		cleanup_and_exit(dcgm_ret, &dcgmHandle, &groupId, &fieldGroupId);
	}
	
	struct timespec time;
	int n_samples, err;
	Sample * cur_sample;

	Proc_Data * cpu_util;
	Proc_Data * prev_proc_data = NULL;

	Net_Data * net_util;


	/* CREATING METRICS TABLE */
	sqlite3 *db;

	char * db_filename;
	asprintf(&db_filename, "%s/%s.db", output_dir, hostbuffer);
	
	int sql_ret;
	sql_ret = sqlite3_open(db_filename, &db);
	if (sql_ret != SQLITE_OK){
		fprintf(stderr, "COULD NOT OPEN SQL DB at filepath: %s. Exiting...\n", db_filename);
		cleanup_and_exit(-1, &dcgmHandle, &groupId, &fieldGroupId);
	}
	free(db_filename);

	char * create_table_cmd = "CREATE TABLE IF NOT EXISTS Data (timestamp INT, device_id INT, field_id INT, value INT);";
	char * sqlErr;

	sql_ret = sqlite3_exec(db, create_table_cmd, NULL, NULL, &sqlErr);
	if (sql_ret != SQLITE_OK){
		fprintf(stderr, "SQL Error: %s\n", sqlErr);
		cleanup_and_exit(-1, &dcgmHandle, &groupId, &fieldGroupId);
	}

	/* CREATING JOBS TABLE */
	const char * jobs_table_creation = "CREATE TABLE IF NOT EXISTS Jobs ("
							 "job_id INT, "
							 "user_name VARCHAR(10), "
							 "group_name VARCHAR(20), "
							 "n_nodes INT, "
							 "n_cpus INT, "
							 "n_gpus INT, "
							 "mem_mb INT, "
							 "billing INT, "
							 "time_limit CHAR(8), "
							 "submit_time CHAR(19), "
							 "node_list VARCHAR(255), "
							 "start_time CHAR(19), "
							 "end_time CHAR(19), "
							 "elapsed_time CHAR(8), "
							 "state VARCHAR(20), "
							 "exit_code CHAR(3), "
							 "PRIMARY KEY (job_id)"
							 ");";

		sql_ret = sqlite3_exec(db, jobs_table_creation, NULL, NULL, &sqlErr);
	if (sql_ret != SQLITE_OK){
		fprintf(stderr, "SQL Error: %s\n", sqlErr);
		cleanup_and_exit(-1, &dcgmHandle, &groupId, &fieldGroupId);
	}

	
	long time_sec;
		long prev_job_collection_time = 0;
	
	struct timespec iter_end;


	// For now, run indefinitely 
	while (true){
		n_samples = samples_buffer -> n_samples;
		clock_gettime(CLOCK_REALTIME, &time);

		// CHECK TO SEE IF IT HAS BEEN AN HOUR SINCE LAST JOB STATUS QUERY
				// IF SO, CALL PYTHON SCRIPT TO COLLECT INFO FROM SACCT AND DUMP TO DIFFERENT DB
		time_sec = time.tv_sec;
		if (TO_COLLECT_JOB_STATS && ((time_sec - prev_job_collection_time) > JOBS_STATS_FREQ_SEC)){
			collect_job_stats(db, output_dir, hostbuffer, time_sec);
			prev_job_collection_time = time_sec;
		}

		cur_sample = &((samples_buffer -> samples)[n_samples]);
		cur_sample -> time = time;
		
		// COLLECT CPU FREE MEM AND COMPUTE %
		cpu_util = process_proc_stat(cur_sample, prev_proc_data);
		cur_sample -> cpu_util = cpu_util;

		// set the previous to be current so as to accurately compute util % next time
		prev_proc_data = cpu_util;

		// COLLECT NETWORK DATA
		net_util = process_net_stat(cur_sample, samples_buffer -> interface_totals);
		cur_sample -> net_util = net_util;

		// COLLECT GPU VALUES
		
		if (PRINT) {
			printf("Time %ld: Collecting Values...\n", time.tv_sec);
		}

		// update fields (and wait for return)
		dcgm_ret = dcgmUpdateAllFields(dcgmHandle, 1);
		if (dcgm_ret != DCGM_ST_OK){
			fprintf(stderr, "UPDATE ALL FIELDS ERROR, Exiting...\n");
						cleanup_and_exit(dcgm_ret, &dcgmHandle, &groupId, &fieldGroupId);
		}

		// retrieve values
		dcgm_ret = dcgmGetLatestValues(dcgmHandle, groupId, fieldGroupId, &copy_field_values_function, (void *) samples_buffer);
		
		if (dcgm_ret != DCGM_ST_OK){
			fprintf(stderr, "GET LATEST VALUES ERROR, Exiting...\n");
			cleanup_and_exit(dcgm_ret, &dcgmHandle, &groupId, &fieldGroupId);
		}
		
		if (PRINT) {
			if (cpu_util != NULL){
				printf("CPU Stats. Util: %d, Free Mem: %d\n\nGPU Stats:\n", (int) round(cur_sample -> cpu_util -> util_pct), (int) (cur_sample -> cpu_util -> free_mem));
			}
			else{
				printf("Could not retrieve CPU stats\n");
			}
			if (net_util != NULL){
				printf("Net Stats. IB Rx: %ld, IB Tx: %ld, Eth Rx: %ld, Eth Tx: %ld\n", cur_sample -> net_util -> ib_rx_bytes, cur_sample -> net_util -> ib_tx_bytes, cur_sample -> net_util -> eth_rx_bytes, cur_sample -> net_util -> eth_tx_bytes);
			}
		}
		
		if (PRINT) {
			void * field_values = cur_sample -> field_values;
			int ind;
			unsigned short fieldId, fieldType;
			for (int gpuId = 0; gpuId < n_devices; gpuId++){
				for (int fieldNum = 0; fieldNum < n_fields; fieldNum++){
					ind = gpuId * n_fields + fieldNum;
					fieldId = fieldIds[fieldNum];
					fieldType = fieldTypes[fieldNum];
					switch (fieldType) {
						case DCGM_FT_DOUBLE:
							printf("GPU ID: %d, Field ID: %u, Value: %d\n", gpuId, fieldId, (int) round((((double *) field_values)[ind] * 100)));
							break;
						case DCGM_FT_INT64:
							printf("GPU ID: %d, Field ID: %u, Value: %d\n", gpuId, fieldId, (int) (((long *) field_values)[ind]));
							break;
						case DCGM_FT_TIMESTAMP:
							printf("GPU ID: %d, Field ID: %u, Value: %d\n", gpuId, fieldId, (int) (((long *) field_values)[ind]));
							break;
						default:
							printf("Error in Field Value Types...");
							printf("GPU ID: %d, Field ID: %u\n", gpuId, fieldId);
							cleanup_and_exit(dcgm_ret, &dcgmHandle, &groupId, &fieldGroupId);
							break;
					}
				}
				printf("\n");
			}
		}
		
		
		n_samples++;
		samples_buffer -> n_samples = n_samples;
		// SAVING VALUES
		if (n_samples == n_samples_per_buffer){
			err = dump_samples_buffer(samples_buffer, db);
			if (err == -1){
				fprintf(stderr, "Error dumping buffer to file. Skipping this dump and collecting new data...\n");
			}
			samples_buffer -> n_samples = 0;
		}


				//clock_gettime(CLOCK_REALTIME, &iter_end);

		//long start_timestamp = time.tv_sec * 1e9 + time.tv_nsec;
		//long end_timestamp = iter_end.tv_sec * 1e9 + iter_end.tv_nsec;
		//long elapsed_time_ns = end_timestamp - start_timestamp;
		//printf("%ld,%d,%ld,%ld\n", elapsed_time_ns, n_samples, start_timestamp, end_timestamp);

		usleep(update_freq_micros);
	}

	// shouldn't reach this point because inifinte loop collecting data
	// free's field value memory in this funciton
	dump_samples_buffer(samples_buffer, db);

	// destroy the buffer
	free(fieldIds);
	free(fieldTypes);
	free(samples_buffer -> samples);
	free(samples_buffer);
	free(hostbuffer);
	// AT END
	cleanup_and_exit(DCGM_ST_OK, &dcgmHandle, &groupId, &fieldGroupId);

}
