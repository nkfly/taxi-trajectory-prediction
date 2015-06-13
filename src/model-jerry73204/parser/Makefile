SOURCES=parser.c
TARGET=parser
CFLAGS=-g -O3 -Wall -std=gnu11 -fopenmp -DNDEBUG
LDFLAGS=
INCLUDES=
LIBS=-lm

all: $(SOURCES)
	gcc -o $(TARGET) $(CFLAGS) $(SOURCES) $(INCLUDES) $(LDFLAGS) $(LIBS)

clean:
	rm -rf $(TARGET)
