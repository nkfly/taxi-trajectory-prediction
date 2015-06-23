SOURCES=features.c
TARGET=features
CFLAGS=-g -O3 -Wall -std=gnu11 -fopenmp -DNDEBUG
LDFLAGS=
INCLUDES=
LIBS=-lm

all: $(SOURCES)
	gcc -o $(TARGET) $(CFLAGS) $(SOURCES) $(INCLUDES) $(LDFLAGS) $(LIBS)

clean:
	rm -rf $(TARGET)
