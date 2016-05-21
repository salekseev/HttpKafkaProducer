SRCFILES := http_parser.c http_parser.h threadpool.c threadpool.h main.cpp
OBJFILES := main.o http_parser.o threadpool.o
CXX := g++ #-std=c++11
CC := gcc
TARGET = message_producer

$(TARGET): $(OBJFILES)
	$(CXX) $(OBJFILES) -o $@ -lrdkafka++ -lz -lrt -lpthread

$(OBJFILES): $(SRCFILES)
	$(CXX) -DDEBUG -D_REENTRANT -g -c main.cpp
	$(CC) -g -c http_parser.c
	$(CC) -g -c threadpool.c

.PHONY:
clean:.PHONY
	rm -rf $(OBJFILES) $(TARGET)

