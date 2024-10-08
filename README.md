# GPU Server Monitoring

---

### Dependencies
- Nvidia's DCGM Library (libdcgm)
- SQLite3 (libsqlite3)

---

### Description

This monitoring program utilizes Nvidia's DCGM API to retrieve fine-grained, real-time metrics for GPU utilization ***not*** available through ```nvidia-smi``` (or equivalently the NVML API). It also queries the Linux filesystem to retrieve CPU usage, system memory, and network statistics.

This program is meant to be run as a daemon process on GPU server node. **It has minimal overhead itself and will not impact job performance<sup>+</sup>** (Note: see the very bottom for an explanation of a special case. I ran benchmarks confirming 0 overhead in common case and an upper-bounded overhead in the special case, of which I artificially constructed because it is unlikely to ever occur in-the-wild). 

Once the monitoring program is running on the node it will create (if doesn't exist) and populate a SQLite database with server utilization metrics and SLURM job statistics for jobs that have finished running on that node. The database has two tables, ```Data``` (for CPU/GPU/Network metrics) and ```Jobs``` (for jobs that have finished on that node). 

When the sample buffer becomes full, the program will insert a batch of ```num samples per buffer * ((num DCGM fields * num gpus) + 7)``` rows into the Data table within a single SQLite transaction (see below for parameter descriptions/defaults). 

I have uploaded an example notebook, ```analyze_data.ipynb``` which demonstrates converting the GPU metrics saved in the Data table of SQLite DB to a compact numpy representation that can be used for further analysis. 

##### Data Table Schema (per row):
1. **timestamp** 
    - In nanoseconds
2. **device_id**
    - Where -1 == CPU, and [0, num_gpus) refers to the system-defined GPU ID
3. **field_id**
    - Where Field IDs 0, 1, 2, 10, 11, 14, 15 are custom-defined as:
        - 1 = System Memory Utilized %
        - 2 = Free System Memory in MB
        - 3 = CPU Utilization % (averaged across all cores)
        - 10 = Aggregate Bytes Received on all Infiniband Network Devices within sample interval
        - 11 = Same as 10, but Transmitted bytes
        - 14 = Aggregate Bytes Received on all Ethernet interfaces within sample interval
        - 15 = Same as 14, but transmitted
    - Where all other field IDs are defined within [Nvidia DCGM API Ref](https://docs.nvidia.com/datacenter/dcgm/2.0/dcgm-api/group__dcgmFieldIdentifiers.html#group__dcgmFieldIdentifiers) and the populated fields within the database will have been specified as run-time parameter ```--fields or -f``` (which has a default value, see "Run-Time Parameters" below)
      - The default fields (not specifying the --fields parameters) are:
        - The usual ```nvidia-smi``` outtputed util % and memory % numbers:
          - 203: Nvidia-SMI (= NVML API) Coarse Grained GPU Util %
            - Ratio of cycles that ***any*** SM had warp assigned
          - 254: Used Memory %
        - Fine grained DCGM metrics, ***averaged spatially*** across SMs
          - 1002: SM_ACTIVE
              - Ratio of cycles at least 1 warp assigned to any SM
          - 1003: SM_OCCUPANCY
              - Ratio of warps resident to theoretical maximum warps per cycle
          - 1004: PIPE_TENSOR_ACTIVE
              - Ratio of cycles any tensor pipe is active
        - DCGM device metrics:
          - 1005: DRAM_ACTIVE:
              - Ratio of cycles device memory interface is sending or receiving data
          - 1009: PCIe Transmitted Bytes
          - 1010: PCIe Received Bytes
          - 1011: NVLink Transmitted Bytes
          - 1012: NVLink Received Bytes
    - Note: The updated versions of the DCGM API Docs only list the Enum names and not numbers. However the argument parsing aspect of the program assumes comma-separated list of numbers. If you want to specify based on the Enum, then in a separate program you can print out the enum to retrieve the unsigned short value.  
4. **value**
    - The value attributed to each field id. The interpretation of field value is relative to the field id.
    
For nodes within an RDMA network, it queries the ```/sys/class/infiniband/<ib_dev_id>/ports/1/counters/port_[rcv|xmit]_data``` files (valid for Mellanox cards, unknown for other vendors) to retrieve physical network traffic. This assumes that the network setup has made all ports on a given network card separately defined IB Devices (all with a single port).


For nodes within a SLURM cluster, it queries the SLURM database to retrieve jobs that have completed on the same node that the monitoring program running on.

The data within the ```Jobs``` table is populated every hour based on a SLURM ```saccnt``` command hard-coded within ```collect_job_stats()``` function located in the file ```job_stats.c```. Collecting job stats is optional and can be toggled with with a compile-time parameter (see below).

---

## Usage

### Parameters

#### Compile-Time (Optional)
- Within ```monitoring.c``` there are some constants defined at the top:
    - **TO_PRINT**
        - When initially setting up the monitoring it might be helpful to print out all the values the that program will dump to database to ensure that you have it set up things properly. Toggling this to 1 will print out values in real time
    - **DEFAULT_OUTPUT_DIR**
        - You can set a default filesystem path to a directory in which the database will be created (instead of always specifying at runtime). The database's name will be ```<hostname>.db``` within this directory where ```<hostname>``` is retrieved from ```gethostname()``` system call.  
    - **TO_COLLECT_JOB_STATS**
        - If the program is being run on node within a SLURM cluster, you can collect information about slurm jobs that have fishing running on that node. The file 'job_stats.h' has the data structure for the information that is queried from SLURM's ```saccnt``` CLI. The file ```job_stats.c``` has the functions responsible for calling ```saccnt``` and dumping the corresponding results within the jobs database. Jobs are queried every hour and any new jobs that have finished within the hour will be added to the database.

#### Run-Time (Optional)
1. **Output Directory** (specified by --output_dir or -o)
  - This overwrites the value of DEFAULT_OUTPUT_DIR set at compile-time
2. **DCGM Fields** (specified by --fields or -f)
  - A comma-seperated list of DCGM field id's that you would like to query
    - Default value: ```203,254,1002,1003,1004,1005,1009,1010,1011,1012```
      - Note that the custom-defined field ids ```1, 2, 3, 10, 11, 14, 15``` should *NOT* be specified as part of this list and will be collected by default. Any errors retrieving custom-defined fields (e.g. if there are no infiniband devices) will be outputted to ```stderr``` and the values assoicated with these fields are undefined.
3. **Sample Frequency** (specified by --sample_freq_millis or -s)
  - This defines the sample frequency in milliseconds for how often you would like to obtain results from DCGM. The minimum value (= highest_freq) is 100ms. This controls the amount of data being produced on the whole and also the CPU usage for running the monitoring program. After every sample iteration the program sleeps (yields the CPU) for the specified sample frequency.
    - Default value: ```100```
4. **Number of Samples Per Buffer** (specified by --n_samples_per_buffer or -n)
  - This defines the buffer size for holding samples before dumping to the SQLite database. A low buffer size will populate the DB with recent samples and conserve RAM usage in exchange for experiencing poor amortized throughput when writing to the database and consume more aggregate CPU cycles. If the filesystem is on the network then a low buffer size will have to pay high latency overheads. A high buffer size will experience better DB write (and possibly network if DB is over-the-network) throughput and thus conserve CPU cycles. This comes at the cost of higher RAM usage (storing larger buffer) and less recent samples populated in the DB. Note the is a relationship between buffer size and sample frequency. If you do not want to miss any sample intervals then there is an upper-bound for the buffer size which is defined by the duration of time inserting N samples into the DB takes. This should not exceed the sample frequency. 
  
### Running the Tool

After (optionally) setting the build parameters you can run ```make``` in this directory. Then you can run the executable ```./monitor <optional args>``` to start collecting data.
  
--- 

##### <sup>+</sup>*Rare Exception that would cause GPU Job Overhead*
Assume an environment where the monitoring program is running on a node where ***all*** CPU cores are utilized (with no blocking) **and** the GPU kernels dispatched are small. In this case, the monitoring program will be forced to preempt a job's thread (which may have been dispatching small GPU kernels without running ahead). Had the switch not occurred more work would have been submitted and immediately processed (because in this scenario we assumed small GPU kernels) meaning the job would have finished faster and thus the monitoring caused job overhead. This overhead is upper-bounded by the time each sample iteration takes (roughly on order of hundreds of microseconds to single-digit milliseconds -- most of which is spent within the DCGM library calls) divided by the sample frequency (in time units).

However, this scenario is likely to never occur. If all CPU cores are being utilized => heavy job => large GPU kernels => more run-ahead for job's host-thread which is dispatching work => disptaching-thread being preempted by monitoring thread and getting switched out will not impact performance because there is enough work to be done without any more work being submitted. Then the monitoring program will occupy the CPU for a short duration, collect data, and the job's host-thread will continue submitting work and running ahead. During a sample buffer bump the monitoring program will occupy a core for longer, but this is configurable (smaller buffer size => more even distribution of iteration times per collection window, but with higher average). 

Moreover, within a dedicated, high-performance GPU cluster, the assumption that all CPU cores (of which there are likely at least 32-128) have work to do for duration of sample interval = (<= ~a few ms) is extremely rare.
