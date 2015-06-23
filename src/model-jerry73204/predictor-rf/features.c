#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#include <unistd.h>
#include <errno.h>
#include <assert.h>

#define MIN_LONGITUDE -9.729117
#define MAX_LONGITUDE -5.793111

#define MIN_LATITUDE  37.313784
#define MAX_LATITUDE  44.119224

#define GRID_SIZE     0.001

#define FEATURE_POLYLINE_SIZE 400
#define OFFSET_FROM_DEST      4

struct __attribute__((__packed__)) coordinate
{
    double longitude;
    double latitude;
};

struct __attribute__((__packed__)) feature_vector
{
    struct coordinate polyline[FEATURE_POLYLINE_SIZE];
    int month;
    int week_day;
    int time_of_day;
    int class_id;
};

struct dataset
{
    char *mem;
    char *mem_trips;
    struct coordinate *mem_coordinates;
    size_t mem_size;
    int num_trips;
    int max_trip_id_length;
    int trip_chunk_size;
};

struct file_features
{
    struct feature_vector *mem;
    long int mem_size;
    int num_feature_vectors;
};

struct file_labels
{
    int *mem;
    long int mem_size;
    int num_labels;
};

struct sampled_trip
{
    char *mem_ptr;
    struct coordinate *polyline_begin;
    int polyline_size;
    int label;
};

int init_dataset(char *path, struct dataset *dataset)
{
    int ret;

    int fd_dataset = open(path, O_RDWR);
    if (fd_dataset < 0)
        return errno;

    struct stat stat_dataset;
    ret = fstat(fd_dataset, &stat_dataset);
    if (ret < 0)
        return errno;

    dataset->mem = (char*) mmap(NULL, stat_dataset.st_size, PROT_READ, MAP_SHARED | MAP_POPULATE, fd_dataset, 0);
    if (dataset->mem == NULL)
        return errno;

    ret = close(fd_dataset);
    if (ret)
        return errno;

    dataset->mem_size = stat_dataset.st_size;
    dataset->num_trips = *(int*) dataset->mem;
    dataset->max_trip_id_length = *(int*) (dataset->mem + sizeof(int));
    dataset->trip_chunk_size = dataset->max_trip_id_length + sizeof(int) * 9;

    dataset->mem_trips = dataset->mem + sizeof(int) * 2;
    dataset->mem_coordinates = (struct coordinate*) (dataset->mem + sizeof(int) * 2 + (dataset->max_trip_id_length + sizeof(int) * 9) * dataset->num_trips);

    return 0;
}

int destroy_dataset(struct dataset *dataset)
{
    munmap(dataset->mem, dataset->mem_size);
    return 0;
}

int init_file_features(char *path, struct file_features *file_features, int num_feature_vectors)
{
    int ret;

    int fd = open(path, O_RDWR | O_CREAT, 0644);
    if (fd < 0)
        return errno;

    file_features->mem_size = sizeof(struct feature_vector) * num_feature_vectors;

    ret = ftruncate(fd, 0);
    if (ret)
        return errno;

    ret = ftruncate(fd, file_features->mem_size);
    if (ret)
        return errno;

    file_features->mem = (struct feature_vector*) mmap(NULL, file_features->mem_size, PROT_WRITE, MAP_SHARED, fd, 0);
    if (file_features->mem == NULL)
        return errno;

    ret = close(fd);
    if (ret)
        return errno;

    file_features->num_feature_vectors = num_feature_vectors;
    return 0;
}

int destroy_file_features(struct file_features *file_features)
{
    munmap(file_features->mem, file_features->mem_size);
    return 0;
}

int init_file_labels(char *path, struct file_labels *file_labels, int num_labels)
{
    int ret;

    int fd = open(path, O_RDWR | O_CREAT, 0644);
    if (fd < 0)
        return errno;

    file_labels->mem_size = sizeof(int) * num_labels;

    ret = ftruncate(fd, 0);
    if (ret)
        return errno;

    ret = ftruncate(fd, file_labels->mem_size);
    if (ret)
        return errno;

    file_labels->mem = (int*) mmap(NULL, file_labels->mem_size, PROT_WRITE, MAP_SHARED, fd, 0);
    if (file_labels->mem == NULL)
        return errno;

    ret = close(fd);
    if (ret)
        return errno;

    file_labels->num_labels = num_labels;
    return 0;
}

int destroy_file_labels(struct file_labels *file_labels)
{
    munmap(file_labels->mem, file_labels->mem_size);
    return 0;
}

int coordinate_to_label(struct coordinate *c)
{
    assert(c->longitude <= MAX_LONGITUDE && c->longitude >= MIN_LONGITUDE);
    assert(c->latitude <= MAX_LATITUDE && c->latitude >= MIN_LATITUDE);

    int index_longitude = (int) (c->longitude / GRID_SIZE) - (int) (MIN_LONGITUDE / GRID_SIZE);
    int index_latitude = (int) (c->latitude / GRID_SIZE) - (int) (MIN_LATITUDE / GRID_SIZE);

    return ((int) (MAX_LONGITUDE / GRID_SIZE) - (int) (MIN_LONGITUDE / GRID_SIZE)) * index_latitude + index_longitude;
}

int main(int argc, char **argv)
{
    if (argc != 6)
    {
        fprintf(stderr, "Usage: %s train_file test_file out_train_features out_train_labels out_test_features\n", argv[0]);
        return 1;
    }

    int ret;

    struct dataset dataset_train;
    ret = init_dataset(argv[1], &dataset_train);
    if (ret)
        return ret;

    /* sample trips and discard corrupted ones */
    struct sampled_trip *sampled_trips = malloc(sizeof(struct sampled_trip) * dataset_train.num_trips);
    if (sampled_trips == NULL)
        return errno;
    struct sampled_trip *sampled_trips_end = sampled_trips;

#pragma omp parallel for schedule(dynamic)
    for (char *mem_ptr = dataset_train.mem_trips;
         mem_ptr < (char*) dataset_train.mem_coordinates;
         mem_ptr += dataset_train.trip_chunk_size)
    {
        /* copy polyline as features */
        int polyline_size = *(int*) (mem_ptr + dataset_train.max_trip_id_length + sizeof(int) * 7);
        if (polyline_size <= OFFSET_FROM_DEST)
            continue;

        int polyline_index = *(int*) (mem_ptr + dataset_train.max_trip_id_length + sizeof(int) * 8);
        struct coordinate *polyline = &dataset_train.mem_coordinates[polyline_index];
        struct coordinate *feature_polyline_end = &polyline[polyline_size - OFFSET_FROM_DEST];
        struct coordinate *feature_polyline_begin = feature_polyline_end - FEATURE_POLYLINE_SIZE;
        if (feature_polyline_begin < polyline)
            feature_polyline_begin = polyline;

        struct sampled_trip *trip = __sync_fetch_and_add(&sampled_trips_end, sizeof(struct sampled_trip));
        trip->mem_ptr = mem_ptr;
        trip->polyline_begin = feature_polyline_begin;
        trip->polyline_size = feature_polyline_end - feature_polyline_begin;
        trip->label = coordinate_to_label(&polyline[polyline_size - 1]);
    }

    /* create feature matrix and label matrix */
    int num_sampled_trips = sampled_trips_end - sampled_trips;

    struct file_features features_train;
    ret = init_file_features(argv[3], &features_train, num_sampled_trips);
    if (ret)
        return ret;

    struct file_labels labels_train;
    ret = init_file_labels(argv[4], &labels_train, num_sampled_trips);
    if (ret)
        return ret;

#pragma omp parallel for schedule(dynamic)
    for (struct sampled_trip *trip = &sampled_trips[0];
         trip < sampled_trips_end;
         trip++)
    {
        char *mem_ptr = trip->mem_ptr;
        int index = trip - &sampled_trips[0];
        struct feature_vector *vector = &features_train.mem[index];

        /* write label */
        labels_train.mem[index] = trip->label;

        /* copy polyline as features */
        memcpy(&vector->polyline[FEATURE_POLYLINE_SIZE - trip->polyline_size],
               trip->polyline_begin,
               trip->polyline_size * sizeof(struct coordinate));

        /* calc time features */
        time_t timestamp = *(time_t*) (mem_ptr + dataset_train.max_trip_id_length + sizeof(int) * 4);
        struct tm time;
        gmtime_r(&timestamp, &time);

        vector->month = time.tm_mon;
        vector->week_day = time.tm_wday;
        vector->time_of_day = time.tm_hour * 3600 + time.tm_min * 60 + time.tm_sec;
    }

    destroy_dataset(&dataset_train);
    destroy_file_features(&features_train);
    destroy_file_labels(&labels_train);

    /* create feature matrix for test dataset */
    struct dataset dataset_test;
    ret = init_dataset(argv[2], &dataset_test);
    if (ret)
        return ret;

    struct file_features features_test;
    ret = init_file_features(argv[5], &features_test, dataset_test.num_trips);
    if (ret)
        return ret;

#pragma omp parallel for schedule(dynamic)
    for (char *mem_ptr = dataset_test.mem_trips;
         mem_ptr < (char*) dataset_test.mem_coordinates;
         mem_ptr += dataset_test.trip_chunk_size)
    {
        int index = (mem_ptr - dataset_test.mem_trips) / dataset_test.trip_chunk_size;
        struct feature_vector *vector = &features_test.mem[index];

        /* copy polyline as features */
        int polyline_size = *(int*) (mem_ptr + dataset_test.max_trip_id_length + sizeof(int) * 7);
        int polyline_index = *(int*) (mem_ptr + dataset_test.max_trip_id_length + sizeof(int) * 8);
        struct coordinate *polyline = &dataset_test.mem_coordinates[polyline_index];

        struct coordinate *feature_polyline_begin = &polyline[polyline_size] - FEATURE_POLYLINE_SIZE;
        int feature_polyline_size = FEATURE_POLYLINE_SIZE;

        if (polyline_size < FEATURE_POLYLINE_SIZE)
        {
            feature_polyline_size = polyline_size;
            feature_polyline_begin = polyline;
        }

        memcpy(&vector->polyline[FEATURE_POLYLINE_SIZE - feature_polyline_size],
               feature_polyline_begin,
               feature_polyline_size * sizeof(struct coordinate));

        /* calc time features */
        time_t timestamp = *(time_t*) (mem_ptr + dataset_test.max_trip_id_length + sizeof(int) * 4);
        struct tm time;
        gmtime_r(&timestamp, &time);

        vector->month = time.tm_mon;
        vector->week_day = time.tm_wday;
        vector->time_of_day = time.tm_hour * 3600 + time.tm_min * 60 + time.tm_sec;
    }

    destroy_dataset(&dataset_test);
    destroy_file_features(&features_test);
    return 0;
}
