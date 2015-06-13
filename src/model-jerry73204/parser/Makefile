SOURCES=parser.c
TARGET=parser
CFLAGS=-g -Wall -std=gnu11 -fopenmp
LDFLAGS=
INCLUDES=
LIBS=-lm

all: $(SOURCES)
	gcc -o $(TARGET) $(CFLAGS) $(SOURCES) $(INCLUDES) $(LDFLAGS) $(LIBS)

clean:
	rm -rf $(TARGET)
