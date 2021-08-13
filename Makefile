LIBRARY_INCLUDE=./src
LIBRARY_PATH=./
SRC_PATH=./src
TESTS_PATH=./tests
OPT=-O3 -DDEBUG -std=c++11
#OPT=-g -DDEBUG -std=c++11

all: textsort test

libtextsort.a:
	g++ -c $(SRC_PATH)/allocation.cc -o allocation.o $(OPT) -I$(LIBRARY_INCLUDE)
	g++ -c $(SRC_PATH)/disk_writer.cc -o disk_writer.o $(OPT) -I$(LIBRARY_INCLUDE)
	g++ -c $(SRC_PATH)/text_sort.cc -o text_sort.o $(OPT) -I$(LIBRARY_INCLUDE)
	g++ -c $(SRC_PATH)/file_io.cc -o file_io.o $(OPT) -I$(LIBRARY_INCLUDE)
	g++ -c $(SRC_PATH)/sort.cc -o sort.o $(OPT) -I$(LIBRARY_INCLUDE)
	g++ -c $(SRC_PATH)/tasks.cc -o tasks.o $(OPT) -I$(LIBRARY_INCLUDE)
	g++ -c $(SRC_PATH)/combine.cc -o combine.o $(OPT) -I$(LIBRARY_INCLUDE)
	g++ -c $(SRC_PATH)/task_message.cc -o task_message.o $(OPT) -I$(LIBRARY_INCLUDE)
	g++ -c $(SRC_PATH)/merge_scheduler.cc -o merge_scheduler.o $(OPT) -I$(LIBRARY_INCLUDE)
	g++ -c $(SRC_PATH)/thread.cc -o thread.o $(OPT) -I$(LIBRARY_INCLUDE)
	g++ -c $(SRC_PATH)/util.cc -o util.o $(OPT) -I$(LIBRARY_INCLUDE)
	g++ -c $(SRC_PATH)/metrics.cc -o metrics.o $(OPT) -I$(LIBRARY_INCLUDE)
	ar -crv libtextsort.a allocation.o text_sort.o file_io.o sort.o tasks.o combine.o \
					disk_writer.o task_message.o merge_scheduler.o thread.o util.o metrics.o

textsort: libtextsort.a
	g++ -c $(SRC_PATH)/textsort.cc -o textsort.o $(OPT) -I$(LIBRARY_INCLUDE)
	g++ -o textsort $(OPT) textsort.o -L$(LIBRARY_PATH) -ltextsort -lpthread

test: libtextsort.a
	g++ -c $(TESTS_PATH)/test.cc -o test.o $(OPT) -I$(LIBRARY_INCLUDE)
	g++ -o test $(OPT) test.o -L$(LIBRARY_PATH) -ltextsort -lpthread

clean:
	rm -rf *.o
	rm -rf *.a
	rm -rf textsort
	rm -rf test
