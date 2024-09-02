CC = gcc
CFLAGS = -O2 -std=c99 -Wall -pedantic

all: monitor

monitor: monitoring.c job_stats.c
	${CC} ${CFLAGS} -o $@ $^ -lsqlite3 -ldcgm -lm

clean:
	rm -f monitor *.o
