/*
	wdantiparkd - A anti-intellipark daemon
	(C) 2010 Sound <sound ~at~ sagaforce -dot- com>

	wdantiparkd
	===========

	wdantiparmd combines the idea of laptop-mode and hard drive touching
	to help keep the power usage low while maintaining a more reasonable 
	load cycle count.

	wdantiparmd is great for NAS servers which are infrequently accessed.
	
	Here's how it work. wdantiparmd runs as a daemon which monitors disk
	activity. It runs in 3 states: ANTI-PARK, PARKED and SLEEP.

	In ANTI-PARK state, the daemon makes disk access every 7 seconds to
	prevent parking. The disk is continuously touched until there is no
	read activities for 1 min. Disk data is synced periodically in this
	state. Once 1 min of read-idleness has passed, it enters PARKED state.

	In PARKED state, the daemon ceases it's disk access and allows the HD
	head to returned to parked position (thus increasing cycle count by 1).
	With laptop-mode enabled, all writes are buffered into RAM. This state
	lasts for 5 mins. If disk activity occurs during these 5 minutes, then
	it resumes back into ANTI-PARK state, except this time does it for 2 mins
	and doubles everytime PARKED state is interrupted (up to a maximum). If
	no disk activities occurs throughout PARKED state, then enter IDLE state.

	In IDLE state, the operation is the same as PARKED state, except that any 
	interruptions returns to the ANTI-PARK state with the default 1 minutes timeout.
	Also, in IDLE state, disk spindown may occur if your kernel supports it.
*/

/*
	This program is free software: you can redistribute it and/or modify
	it under the terms of the GNU General Public License as published by
	the Free Software Foundation, either version 3 of the License, or
	(at your option) any later version.

	This program is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU General Public License for more details.

	You should have received a copy of the GNU General Public License
	along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
#include <getopt.h>
#include <errno.h>
#include <signal.h>
#include <time.h>

// global parameters
struct wdAntiParkConfig
{
	char disk[16];
	char tempFile[128];
	int verbose;
	int interval;
	int antiParkTimeout; 
	int antiParkTimeoutMax;
	int parkedTimeout;
	int syncBeforeIdle;
};

enum AntiParkState
{
	AntiPark,
	Parked,
	Idle
};

static const char *formatSeconds(time_t secs,char *buffer,int max)
{
	static char format[32];
	if(!buffer) {
		buffer = format;
		max = 32;
	}

	if(secs < 60) snprintf(buffer,max,"%lds",(long)secs);
	else if(secs < 3600) snprintf(buffer,max,"%ldm %lds",(long)secs / 60,(long)secs % 60);
	else if(secs < 86400) snprintf(buffer,max,"%ldh %ldm %lds",(long)secs / 3600,(long)(secs / 60) % 60,(long)secs % 60);
	else snprintf(buffer,max,"%ldd %ldh %ldm %lds",(long)secs / 86400,(long)(secs / 3600) % 24,(long)(secs / 60) % 60,(long)secs % 60);
	buffer[max - 1] = 0;
	return buffer;
}

static const char *formatCurrentTime(char *buffer,int max)
{
	static char format[32];
	if(!buffer) {
		buffer = format;
		max = 32;
	}
	time_t curTime = time(NULL);
	strftime(buffer,max,"%a, %b %e  %T",localtime(&curTime));
	buffer[max - 1] = 0;
	return buffer;
}

/*
 Checks for disk activity since the last call to checkForDiskActivity()
 */
int checkForDiskActivity(const char *disk,int *haveReadAcitvity,int *haveWriteActivity)
{
	static unsigned long lastReadSectorCount = 0, lastWriteSectorCount = 0;
	unsigned long readSectorCount, writeSectorCount;
	char statsPath[256];
	char statsLine[512];
	char *value;
	
	// kernel 2.6.. read from /sys
	snprintf(statsPath,256,"/sys/block/%s/stat",disk);
	statsPath[255] = 0;
	
	// read stats
	int diskStatFp = open(statsPath,O_RDONLY);
	if(diskStatFp < 0) {
		fprintf(stderr,"Could not open '%s' stats for reading.\n",disk);
		return -errno;
	}
	read(diskStatFp,statsLine,512);
	statsLine[511] = 0;
	close(diskStatFp);
	
	// split the lines
	strtok(statsLine," "); // read I/Os
	strtok(NULL," "); // read merges
	value = strtok(NULL," ");
	if(!value) {
		fprintf(stderr,"Failed to read I/O stats.\n");
		return -1;
	}
	readSectorCount = strtoul(value,NULL,10);
	
	strtok(NULL," "); // write I/Os
	strtok(NULL," "); // write merges
	value = strtok(NULL," ");
	if(!value) {
		fprintf(stderr,"Failed to read I/O stats.\n");
		return -1;
	}
	writeSectorCount = strtoul(value,NULL,10);
	
	if(haveReadAcitvity) *haveReadAcitvity = readSectorCount != lastReadSectorCount;
	if(haveWriteActivity) *haveWriteActivity = writeSectorCount != lastWriteSectorCount;
	
	lastReadSectorCount = readSectorCount;
	lastWriteSectorCount = writeSectorCount;
	
	return 0;
}

// the loop that does it all
int wdAntiParkRun(struct wdAntiParkConfig *config)
{
	enum AntiParkState state = AntiPark;
	time_t timeoutCountBegin, stateTimeBegin, antiParkStart, idleTime;
	int llc = 0; // estimate llc
	
	// current antipark timeout
	int antiParkTimeout = config->antiParkTimeout;
	
	idleTime = 0; // count idle time
	antiParkStart = time(NULL); // start time of when anti park is executed
	timeoutCountBegin = time(NULL); // timeout timer
	stateTimeBegin = time(NULL); // state timer
	if(config->verbose) {
		printf("[%s] Starting wdantiparkd.\n",formatCurrentTime(NULL,0));
		printf("[%s] Settings:\n",formatCurrentTime(NULL,0));
		printf("[%s]  Interval: %s\n",formatCurrentTime(NULL,0),formatSeconds(config->interval,NULL,0));
		printf("[%s]  AntiPark Timeout: %s\n",formatCurrentTime(NULL,0),formatSeconds(config->antiParkTimeout,NULL,0));
		printf("[%s]  AntiPark Timeout Max: %s\n",formatCurrentTime(NULL,0),formatSeconds(config->antiParkTimeoutMax,NULL,0));
		printf("[%s]  Parked Timeout: %s\n",formatCurrentTime(NULL,0),formatSeconds(config->parkedTimeout,NULL,0));
		printf("[%s]  Sync before IDLE: %s\n",formatCurrentTime(NULL,0),config->syncBeforeIdle ? "true" : "false");
	}
	
	// infinite loop
	for(;;) {
		int haveReadActivity, haveWriteActivity;
		
		// check for disk activity
		checkForDiskActivity(config->disk,&haveReadActivity,&haveWriteActivity);
		
		switch(state) {
			case AntiPark:
				// if there is read activity, reset timeout count
				if(haveReadActivity) {
					timeoutCountBegin = time(NULL);
				}
				
				// write some random data, and sync to keep head's unparked
				int tmpFileFp = open(config->tempFile,O_WRONLY | O_TRUNC | O_CREAT,0600);
				if(tmpFileFp < 0) {
					fprintf(stderr,"Failed to open tmp file '%s' for writing.\n",config->tempFile);
					return -errno;
				}
				write(tmpFileFp,&antiParkStart,4);
				close(tmpFileFp);
				sync(); // force sync
				
				if((time(NULL) - timeoutCountBegin) > antiParkTimeout) {
					if(config->verbose) {
						printf("[%s] Switching state to PARKED. Time spent in ANTIPARK: %s.\n",formatCurrentTime(NULL,0),formatSeconds(time(NULL) - stateTimeBegin,NULL,0));
					}
					timeoutCountBegin = time(NULL);
					stateTimeBegin = time(NULL);
					state = Parked;
					
					sleep(1);

					// sync stats
					checkForDiskActivity(config->disk,NULL,NULL);
					
					// llc + 1
					llc++;
				}
				break;
			case Parked:
				if(haveReadActivity || haveWriteActivity) {
					// if PARKED is interrupted, then restart ANTIPARK with timeout * 2
					antiParkTimeout *= 2; 
					if(antiParkTimeout > config->antiParkTimeoutMax) 
						antiParkTimeout = config->antiParkTimeoutMax;
					
					if(config->verbose) {
						char timeoutStr[32], timeSpentStr[32];
						time_t parkedTime = time(NULL) - stateTimeBegin;
						idleTime += parkedTime;
						printf("[%s] Disk activity detected, switching out of PARKED state to ANTIPARK with timeout: %s. Time spent in PARKED: %s.\n",
							   formatCurrentTime(NULL,0),formatSeconds(antiParkTimeout,timeoutStr,32),formatSeconds(parkedTime,timeSpentStr,32));
					}
					
					timeoutCountBegin = time(NULL);
					stateTimeBegin = time(NULL);
					state = AntiPark;
					continue;
				} else {
					if((time(NULL) - timeoutCountBegin) > config->parkedTimeout) {
						if(config->verbose) {
							time_t parkedTime = time(NULL) - stateTimeBegin;
							idleTime += parkedTime;
							printf("[%s] Switching state to IDLE. Time spent in PARKED: %s.\n",formatCurrentTime(NULL,0),formatSeconds(parkedTime,NULL,0));
						}
						
						// reset counters
						timeoutCountBegin = time(NULL);
						stateTimeBegin = time(NULL);
						state = Idle;
						
						if(config->syncBeforeIdle) {
							printf("[%s] Syncing disks.\n",formatCurrentTime(NULL,0));
							// sync disk first
							sync();
							sleep(1);
							
							// sync stats
							checkForDiskActivity(config->disk,NULL,NULL);
							
							llc++;
						}
						
						// change states reset timers
						timeoutCountBegin = time(NULL);
						stateTimeBegin = time(NULL);
						state = Idle;
						continue;
					} 
				}
				break;
			case Idle:
				if(!haveReadActivity && !haveWriteActivity) break;
				
				antiParkTimeout = config->antiParkTimeout;
				
				if(config->verbose) {
					char timeoutStr[32], timeSpentStr[32];
					time_t parkedTime = time(NULL) - stateTimeBegin;
					time_t uptime = time(NULL) - antiParkStart;
					int hours = (uptime / 3600);
					int llc_per_hour = hours ? llc / hours : llc;
					idleTime += parkedTime;
					printf("[%s] Disk activity detected, switching out of IDLE state to ANTIPARK with timeout: %s. Time spent in IDLE: %s.\n",
						   formatCurrentTime(NULL,0),formatSeconds(antiParkTimeout,timeoutStr,32),formatSeconds(parkedTime,timeSpentStr,32));
					printf("[%s] Current stats - uptime: %s, ",formatCurrentTime(NULL,0),formatSeconds(uptime,NULL,0));
					printf("idle time: %s, ",formatSeconds(idleTime,NULL,0));
					printf("%% idle: %ld%%, ",idleTime * 100 / uptime);
					printf("est. LLC/hr: %d\n",llc_per_hour);
				}
				
				// change states reset timers
				timeoutCountBegin = time(NULL);
				stateTimeBegin = time(NULL);
				state = AntiPark;
				continue;
		}
		
		// sleep for interval seconds
		sleep(config->interval);
	}
}

int main(int argc,char *argv[])
{
	static struct option longOptions[] =
	{
		{ "help", no_argument, NULL, 'h' },
		{ "verbose", no_argument, NULL, 'v' },
		{ "disk", required_argument, NULL, 'd' },
		{ "interval", required_argument, NULL, 'i' },
		{ "antipark-timeout", required_argument, NULL, 'a' },
		{ "antipark-timeout-max", required_argument, NULL, 'A' },
		{ "parked-timeout", required_argument, NULL, 'p' },
		{ "temp-file", required_argument, NULL, 't' },
		{ "sync-before-idle", no_argument, NULL, 'z' },
		{ 0, 0, 0, 0 }
    };

	struct wdAntiParkConfig config = {
		"sda", // disk
		"/tmp/wdantiparkd.tmp",
		0, // verbose
		7, // interval
		60, // antiParkTimeout 
		300, // antiParkTimeoutMax
		300, // parkedTimeout
		0 // syncBeforeIdle
	};
	
	int optionIndex;
	int c;
	while((c = getopt_long(argc,argv,"hvd:i:a:A:p:P:t:z",longOptions,&optionIndex)) != -1) {
		switch(c) {
			case 'v': 
				config.verbose = 1; 
				break;
			case 'd':
				if(strlen(optarg) > 15) {
					fprintf(stderr,"Filename of disk is too long (15 chars max).\n");
					return -1;
				}
				strncpy(config.disk,optarg,16); 
				config.disk[15] = 0; 
				break;
			case 'i': 
				config.interval = strtol(optarg,NULL,10);
				if(config.interval < 0 || config.interval > 3600) {
					fprintf(stderr,"Invalid interval specified by -i, --interval.\n");
					return -1;
				}
				break;
			case 'a':
				config.antiParkTimeout = strtol(optarg,NULL,10);
				if(config.antiParkTimeout < 0 || config.antiParkTimeout > 3600) {
					fprintf(stderr,"Invalid timeout specified by -a, --antipark-timeout.\n");
					return -1;
				}
				break;
			case 'A':
				config.antiParkTimeoutMax = strtol(optarg,NULL,10);
				if(config.antiParkTimeoutMax < 0 || config.antiParkTimeoutMax > 3600) {
					fprintf(stderr,"Invalid timeout specified by -A, --antipark-timeout-max.\n");
					return -1;
				}
				break;
			case 'p':
				config.parkedTimeout = strtol(optarg,NULL,10);
				if(config.antiParkTimeoutMax < 0 || config.antiParkTimeoutMax > 3600) {
					fprintf(stderr,"Invalid timeout specified by -p, --parked-timeout.\n");
					return -1;
				}
				break;
			case 't':
				if(strlen(optarg) > 127) {
					fprintf(stderr,"Filename of temp-file is too long.\n");
					return -1;
				}
				strncpy(config.tempFile,optarg,128);
				config.tempFile[127] = 0;
				break;
			case 'z':
				config.syncBeforeIdle = 1;
				break;
			default:
				printf("wdantiparkd v1.0beta1\n");
				printf("Usage: wdantiParkd [options...]\n");
				printf("Options:\n");
				printf(" -h, --help                     Display this help.\n");
				printf(" -v, --verbose                  Be verbose.\n");
				printf(" -d, --disk=DISK                Disk to monitor (default: %s)\n",config.disk);
				printf(" -i, --interval=SEC             Interval between generated disk activity (default: %d)\n",config.interval);
				printf(" -a, --antipark-timeout=SEC     Timeout for antipark (default: %d)\n",config.antiParkTimeout);
				printf(" -A, --antipark-timeout-max=SEC Timeout max for antipark (default: %d)\n",config.antiParkTimeoutMax);
				printf(" -p, --park-timeout=SEC         Timeout for parked (default: %d)\n",config.parkedTimeout);
				printf(" -t, --temp-file=FILE           File residing on disk to write to (default: %s)\n",config.tempFile);
				printf(" -z, --sync-before-idle         Sync disks before switching to IDLE (default: %s)\n",config.syncBeforeIdle ? "true" : "false");
				return -1;
		}
	}
	
	return wdAntiParkRun(&config);
}

