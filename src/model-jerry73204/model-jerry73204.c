#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <fcntl.h>
#include <errno.h>
#include <math.h>
#include <omp.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>

#ifdef USE_CUDA
#include <cuda.h>
#include <cuda_runtime_api.h>
#endif

#include "model-jerry73204.h"

#define MAX_NUM_METAS 63
#define MAX_NUM_TRIPS 8388608
#define MAX_NUM_POSITIONS 134217728
#define POST_TEST_POLYLINE_LENTH 16
#define MAX_NUM_PREDICTIONS 320

#define SWAP(a, b)                                            \
    do                                                        \
    {                                                         \
        *((a)) ^= *((b));                                     \
        *((b)) ^= *((a));                                     \
        *((a)) ^= *((b));                                     \
    }                                                         \
    while (0);

#define SWAP_POINTER(a, b)                                \
    do                                                    \
    {                                                     \
        typeof (*((a))) tmp = *((b));                     \
        *((b)) = *((a));                                  \
        *((a)) = tmp;                                     \
    }                                                     \
    while (0);

struct meta metas[MAX_NUM_METAS];
int num_metas;

struct trip *train_trips;
int num_train_trips;
struct trip *train_trip_pointers[MAX_NUM_TRIPS];

struct trip *test_trips;
int num_test_trips;
struct trip *test_trip_pointers[MAX_NUM_TRIPS];

struct coordinate *train_positions;
struct coordinate *test_positions;

struct prediction predictions[MAX_NUM_PREDICTIONS];

/* find distance b/w two coordinate */
double distance_coordinate(struct coordinate *line_left, struct coordinate *line_right)
{
    double longitude_diff = line_left->longitude - line_right->longitude;
    double latitude_diff = line_left->latitude - line_right->latitude;
    return sqrt(longitude_diff * longitude_diff + latitude_diff * latitude_diff);
}

/* find the norm b/w two polylines */
double compare_polyline(struct coordinate *line_left, struct coordinate *line_right, int n)
{
    assert(n > 0);
    double norm = 0.0;

    for (int i = 0; i < n; i++)
    {
        norm += distance_coordinate(line_left, line_right);
        line_left++;
        line_right++;
    }
    return norm;
}

/* find minimum norm b/w two trips */
double distance_trip(struct trip *trip_left, struct trip *trip_right)
{
    int size_left = trip_left->polyline_size;
    int size_right = trip_right->polyline_size;

    if (size_left > size_right)
    {
        SWAP(&size_left, &size_right);
        SWAP_POINTER(&trip_left, &trip_right);
    }

    if (size_left == 0)
        return 1.0 / 0.0;

    double min_distance = 1.0 / 0.0; /* infinity */
    struct coordinate *polyline_ptr = trip_left->polyline;
    for (int i = 0; i < size_right - size_left; i++)
    {
        double distance = compare_polyline(polyline_ptr, trip_right->polyline, size_left);
        min_distance = (distance < min_distance ? distance : min_distance );
        polyline_ptr++;
    }

    return min_distance;
}

void load_meta_data(char *path)
{
    int ret;

    /* open file */
    ret = access(path, F_OK) && !access(path, R_OK);
    assert(ret == 0);

    int fd_meta = open(path, O_RDONLY);
    assert(fd_meta >= 0);

    struct stat stat_buf;
    ret = fstat(fd_meta, &stat_buf);
    assert(ret == 0);

    char *mem_begin = (char*) mmap(NULL, stat_buf.st_size, PROT_READ, MAP_SHARED | MAP_POPULATE, fd_meta, 0);
    assert(mem_begin != NULL);
    char *mem_end = mem_begin + stat_buf.st_size;
    ret = close(fd_meta);
    assert(ret == 0);

    char *mem_ptr = strchr(mem_begin, '\n');
    assert(mem_ptr != NULL);
    mem_ptr++;

    /* parse content */
    struct meta *meta_ptr = &metas[0];
    while (mem_ptr != mem_end)
    {
        /* skip index field */
        mem_ptr = strchr(mem_ptr, ',');
        assert(mem_ptr != NULL);
        mem_ptr++;

        /* read location name */
        char *ptr_end = strchr(mem_ptr, ',');
        assert(ptr_end != NULL);
        strncpy(meta_ptr->name, mem_ptr, ptr_end - mem_ptr);
        mem_ptr = ptr_end + 1;

        /* read longitude */
        ptr_end = strchr(mem_ptr, ',');
        assert(ptr_end != NULL);
        meta_ptr->position.longitude = strtod(mem_ptr, NULL);
        assert(errno == 0);
        mem_ptr = ptr_end + 1;

        /* read latitude */
        ptr_end = strchr(mem_ptr, '\n');
        assert(ptr_end != NULL);
        meta_ptr->position.latitude = strtod(mem_ptr, NULL);
        assert(errno == 0);
        mem_ptr = ptr_end + 1;

        meta_ptr++;
    }

    num_metas = meta_ptr - &metas[0];
    ret = munmap(mem_begin, stat_buf.st_size);
    assert(ret == 0);
}

void load_csv_data(char *path, struct trip *trips, struct trip **trip_pointers, struct coordinate *positions, int *num_trips)
{
    int ret;

    /* open file */
    ret = access(path, F_OK) || !access(path, R_OK);
    assert(ret == 0);

    int fd_csv = open(path, O_RDONLY);
    assert(fd_csv >= 0);

    struct stat stat_buf;
    ret = fstat(fd_csv, &stat_buf);
    assert(ret == 0);

    char *mem_begin = (char*) mmap(NULL, stat_buf.st_size, PROT_READ, MAP_SHARED | MAP_POPULATE, fd_csv, 0);
    assert(mem_begin != NULL);
    char *mem_end = mem_begin + stat_buf.st_size;
    ret = close(fd_csv);
    assert(ret == 0);

    /* skip first line */
    char *mem_ptr = strchr(mem_begin, '\n');
    assert(mem_ptr != NULL);
    mem_ptr++;

    int num_chunks;
    int max_num_workers = omp_get_max_threads();
    int min_chunk_size = (mem_end - mem_ptr - 1) / max_num_workers + 1;
    int num_trips_per_worker = MAX_NUM_TRIPS / max_num_workers;
    int num_positions_per_worker = MAX_NUM_POSITIONS / max_num_workers;
    char *begin_ptrs[max_num_workers + 1];
    int trip_count[max_num_workers];
    begin_ptrs[0] = mem_ptr;

    /* compute chunk ranges for each worker */
    assert(max_num_workers > 1);
    for (int i = 1; i <= max_num_workers; i++)
    {
        char *curr_ptr = begin_ptrs[i - 1] + min_chunk_size;
        if (curr_ptr <= mem_end)
        {
            curr_ptr = strchr(curr_ptr, '\n');
            assert(curr_ptr != NULL);
            curr_ptr++;
            begin_ptrs[i] = curr_ptr;
        }
        else
        {
            curr_ptr = mem_end;
            begin_ptrs[i] = curr_ptr;
        }

        if (curr_ptr == mem_end)
        {
            num_chunks = i;
            begin_ptrs[i + 1] = mem_end;
            break;
        }
    }

    /* parse content */
#pragma omp parallel for
    for (int i = 0; i < num_chunks; i++)
    {
        struct trip *trip_begin = &trips[i * num_trips_per_worker];
        struct trip *trip_ptr = trip_begin;
        struct coordinate *position_begin = &positions[i * num_positions_per_worker];
        struct coordinate *position_ptr = &positions[i * num_positions_per_worker];
        char *ptr = begin_ptrs[i];
        char *end = begin_ptrs[i + 1];
        char *str_end;

        while (ptr != end)
        {
            /* read trip id */
            assert(*ptr == '"');
            ptr++;

            str_end = strchr(ptr, '"');
            assert(str_end != NULL);
            strncpy(trip_ptr->trip_id, ptr, str_end - ptr);
            ptr = str_end + 3;

            /* read call type */
            assert(*ptr - 'A' < 3);
            trip_ptr->call_type = *ptr - 'A';
            ptr = strchr(ptr, ',');
            ptr += 2;

            /* read origin call */
            if (*ptr != 'A')
            {
                trip_ptr->origin_call = strtol(ptr, &str_end, 10);
                ptr = str_end + 3;
            }
            else
            {
                trip_ptr->origin_call = 0;
                ptr += 3;
            }

            /* read origin stand */
            if (*ptr != 'A')
            {
                trip_ptr->origin_stand = strtol(ptr, &str_end, 10);
                ptr = str_end + 3;
            }
            else
            {
                trip_ptr->origin_stand = 0;
                ptr += 3;
            }

            /* read taxi id */
            trip_ptr->taxi_id = strtol(ptr, &str_end, 10);
            ptr = str_end + 3;

            /* read timestamp */
            trip_ptr->timestamp = strtol(ptr, &str_end, 10);
            ptr = str_end + 3;

            /* read day type */
            assert(*ptr == 'A');
            trip_ptr->day_type = *ptr - 'A';
            ptr += 4;

            /* read missing data */
            assert(*ptr == 'T' || *ptr == 'F');
            trip_ptr->missing_data = (*ptr == 'T');
            ptr = strchr(ptr, ',');
            ptr += 2;

            /* read polyline */
            assert(*ptr == '[');
            ptr++;

            trip_ptr->polyline = position_ptr;

            while (*ptr == '[')
            {
                ptr++;

                /* read longitude */
                position_ptr->longitude = strtod(ptr, &str_end);
                assert(*str_end == ',');
                ptr = str_end + 1;

                /* read latitude */
                position_ptr->latitude = strtod(ptr, &str_end);
                assert(*str_end == ']');
                ptr = str_end + 2;

                position_ptr++;
            }

            /* compute polyline size */
            assert(*ptr == '"' || *ptr == ']');
            if (*ptr == '"')
            {
                trip_ptr->polyline_size = position_ptr - trip_ptr->polyline;
                ptr += 2;
            }
            else
            {
                trip_ptr->polyline_size = 0;
                ptr += 3;
            }

            trip_ptr++;
        }

        trip_count[i] = trip_ptr - trip_begin;
        assert(trip_count[i] <= num_trips_per_worker);
        assert(position_ptr - position_begin <= num_positions_per_worker);
    }

    ret = munmap(mem_begin, stat_buf.st_size);
    assert(ret == 0);

    /* populate table of pointers to the trips */
    int trip_begin_index[num_chunks + 1];
    trip_begin_index[0] = 0;
    for (int i = 1; i <= num_chunks; i++)
        trip_begin_index[i] = trip_begin_index[i - 1] + trip_count[i - 1];

    *num_trips = trip_begin_index[num_chunks];

#pragma omp parallel for
    for (int i = 0; i < num_chunks; i++)
    {
        struct trip *trip_begin = &trips[i * num_trips_per_worker];
        struct trip *trip_ptr = trip_begin;

        struct trip **trip_pointers_begin = &trip_pointers[trip_begin_index[i]];
        struct trip **trip_pointers_end = trip_pointers_begin + trip_count[i];

        for (struct trip **trip_pointers_ptr = trip_pointers_begin;
             trip_pointers_ptr != trip_pointers_end;
             trip_pointers_ptr++)
        {
            *trip_pointers_ptr = trip_ptr;
            trip_ptr++;
        }
    }

}

#ifdef USE_CUDA
void compute_prediction_cuda()
{
    int device_id;
    int device_count;

    cudaGetDevice(&device_id);
    cudaGetDeviceCount(&device_count);

    /* TODO */
}
#else
void compute_prediction_openmp()
{
    /* find distances b/w trips */
    struct trip **train_trip_pointers_end = &train_trip_pointers[num_train_trips];
    struct trip **test_trip_pointers_end = &test_trip_pointers[num_test_trips];

    /* find nearest training trip for each testing trip */
    for (struct trip **test_pointers_ptr = &test_trip_pointers[0];
         test_pointers_ptr < test_trip_pointers_end;
         test_pointers_ptr++)
    {
        double min_distance = 1.0 / 0.0; /* infinity */
        struct trip *nearest_trip = NULL;
        struct trip *test_ptr = *test_pointers_ptr;


#pragma omp parallel for schedule(dynamic)
        for (struct trip **train_pointers_ptr = &train_trip_pointers[0];
             train_pointers_ptr < train_trip_pointers_end;
             train_pointers_ptr++)
        {
            struct trip *train_ptr = *train_pointers_ptr;
            double distance = distance_trip(test_ptr, train_ptr);
            if (distance < min_distance)
            {
                min_distance = distance;
                nearest_trip = train_ptr;
            }
        }

        int index = test_pointers_ptr - &test_trip_pointers[0];
        predictions[index].test_trip = test_ptr;
        predictions[index].destination = nearest_trip->polyline[nearest_trip->polyline_size - 1];
    }
}
#endif

void print_prediction()
{
    printf("\"TRIP_ID\",\"LATITUDE\",\"LONGITUDE\"\n");
    for (struct prediction *ptr = &predictions[0]; ptr != &predictions[num_test_trips]; ptr++)
        printf("\"%s\",%lf,%lf\n", ptr->test_trip->trip_id, ptr->destination.latitude, ptr->destination.longitude);
}

int main(int argc, char **argv)
{
    assert(argc == 4);

    /* init memory space */
    train_trips = (struct trip*) malloc(MAX_NUM_TRIPS * sizeof(struct trip));
    assert(train_trips != NULL);

    test_trips = (struct trip*) malloc(MAX_NUM_TRIPS * sizeof(struct trip));
    assert(test_trips != NULL);

    train_positions = (struct coordinate*) malloc(MAX_NUM_POSITIONS * sizeof(struct coordinate));
    assert(train_positions != NULL);

    test_positions = (struct coordinate*) malloc(MAX_NUM_POSITIONS * sizeof(struct coordinate));
    assert(test_positions != NULL);

    /* parse dataset */
    load_meta_data(argv[1]);
    load_csv_data(argv[2], train_trips, train_trip_pointers, train_positions, &num_train_trips);
    load_csv_data(argv[3], test_trips, test_trip_pointers, test_positions, &num_test_trips);

    /* make prediction */
#ifdef USE_CUDA
    compute_prediction_cuda();
#else
    compute_prediction_openmp();
#endif

    /* print results */
    print_prediction();

    return 0;
}
