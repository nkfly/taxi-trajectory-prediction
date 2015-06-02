SOURCES=model-jerry73204.c
TARGET=model-jerry73204
CFLAGS=-g -O3 -Wall -std=gnu11 -fopenmp -DNDEBUG
LDFLAGS=
INCLUDES=
LIBS=-lm

LDFLAGS_CUDA=-L/opt/cuda/lib64
INCLUDES_CUDA=-I/opt/cuda/include
LIBS_CUDA=-lcudart

all: $(SOURCES)
	gcc -o $(TARGET) $(CFLAGS) $(SOURCES) $(INCLUDES) $(LDFLAGS) $(LIBS)

use_cuda: $(SOURCES)
	gcc -o $(TARGET) -DUSE_CUDA $(CFLAGS) $(SOURCES) $(INCLUDES) $(INCLUDES_CUDA) $(LDFLAGS) $(LDFLAGS_CUDA) $(LIBS) $(LIBS_CUDA)

clean:
	rm -rf $(TARGET)
