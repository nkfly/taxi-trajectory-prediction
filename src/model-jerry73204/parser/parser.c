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

#include "parser.h"

#define MAX_NUM_METAS 63
#define MAX_NUM_TRIPS 8388608
#define MAX_NUM_POSITIONS 134217728
#define COMPARED_POLYLINE_LENTH 16
#define MAX_NUM_PREDICTIONS 320
#define DOUBLE_INFINITY ((1.0 / 0.0))

#define SWAP(a, b)                              \
    do                                          \
    {                                           \
        *((a)) ^= *((b));                       \
        *((b)) ^= *((a));                       \
        *((a)) ^= *((b));                       \
    }                                           \
    while (0);

#define SWAP_POINTER(a, b)                      \
    do                                          \
    {                                           \
        typeof (*((a))) tmp = *((b));           \
        *((b)) = *((a));                        \
        *((a)) = tmp;                           \
    }                                           \
    while (0);


int max_num_workers;

struct meta metas[MAX_NUM_METAS];
int max_meta_name_length;
int num_metas;

struct trip *train_trips;
int num_train_trips;
int max_train_trip_id_length;
struct trip *train_trip_pointers[MAX_NUM_TRIPS];

struct trip *test_trips;
int num_test_trips;
int max_test_trip_id_length;
struct trip *test_trip_pointers[MAX_NUM_TRIPS];

struct coordinate *train_positions;
struct coordinate *test_positions;

void parse_meta_data(char *path)
{
    int ret;

    /* open file */
    ret = access(path, F_OK) || access(path, R_OK);
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

        int name_length = ptr_end - mem_ptr;
        meta_ptr->name_length = name_length;
        strncpy(meta_ptr->name, mem_ptr, name_length);
        if (name_length > max_meta_name_length)
            max_meta_name_length = name_length;
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

void parse_csv_data(char *path, struct trip *trips, struct trip **trip_pointers, struct coordinate *positions, int *num_trips, int *max_trip_id_length)
{
    int ret;

    /* open file */
    ret = access(path, F_OK) || access(path, R_OK);
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
    int min_chunk_size = (mem_end - mem_ptr - 1) / max_num_workers + 1;
    int num_trips_per_worker = MAX_NUM_TRIPS / max_num_workers;
    int num_positions_per_worker = MAX_NUM_POSITIONS / max_num_workers;
    char *begin_ptrs[max_num_workers + 1];
    int trip_count[max_num_workers];
    begin_ptrs[0] = mem_ptr;

    /* compute chunk ranges for each worker */
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
    int max_id_length = 0;
#pragma omp parallel for reduction(max: max_id_length)
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
            int trip_id_length = str_end - ptr;
            strncpy(trip_ptr->trip_id, ptr, trip_id_length);
            if (max_id_length < trip_id_length)
                max_id_length = trip_id_length;
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

    *max_trip_id_length = max_id_length;
}

void dump_meta_data(char *path)
{
    int ret;

    char name_buffer[MAX_META_NAME_SIZE * num_metas];
    int offsets[num_metas];
    int name_buffer_size;

    char *name_buffer_ptr = &name_buffer[0];
    int *offset_ptr = &offsets[0];
    for (struct meta *ptr = &metas[0]; ptr != &metas[num_metas]; ptr++)
    {
        *offset_ptr = name_buffer_ptr - &name_buffer[0];
        strncpy(name_buffer_ptr, ptr->name, ptr->name_length + 1);
        name_buffer_ptr += ptr->name_length + 1;
        offset_ptr++;
    }
    name_buffer_size = name_buffer_ptr - &name_buffer[0];

    int file_size = sizeof(num_metas) + sizeof(max_meta_name_length) + num_metas * (sizeof(int) + sizeof(double) * 2) + name_buffer_size;

    /* open file */
    int fd_meta_out = open(path, O_RDWR | O_CREAT, 0644);
    assert(fd_meta_out >= 0);

    ret = ftruncate(fd_meta_out, file_size);
    assert(ret == 0);

    char *mem_begin = (char*) mmap(NULL, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_meta_out, 0);
    assert(mem_begin != NULL);
    char *mem_end = mem_begin + file_size;
    char *mem_ptr = mem_begin;

    ret = close(fd_meta_out);
    assert(ret == 0);

    *(int*) mem_ptr = num_metas;
    mem_ptr += sizeof(num_metas);

    *(int*) mem_ptr = max_meta_name_length;
    mem_ptr += sizeof(max_meta_name_length);

    offset_ptr = &offsets[0];
    for (struct meta *ptr = &metas[0]; ptr != &metas[num_metas]; ptr++)
    {
        *(int*) mem_ptr = *offset_ptr;
        mem_ptr += sizeof(int);

        *(double*) mem_ptr = ptr->position.longitude;
        mem_ptr += sizeof(double);

        *(double*) mem_ptr = ptr->position.latitude;
        mem_ptr += sizeof(double);

        offset_ptr++;
    }

    memcpy(mem_ptr, name_buffer, name_buffer_size);

    ret = munmap(mem_begin, file_size);
    assert(ret == 0);
}

void dump_trips(char *path, int num_trips, int max_trip_id_length, struct trip **trips)
{
    int ret;
    int polyline_buffer_size = 0;

    assert(max_trip_id_length > 0);
    max_trip_id_length = 1 << (8 * sizeof(int) - __builtin_clz((unsigned int) (max_trip_id_length - 1)));

#pragma omp parallel for reduction(+: polyline_buffer_size)
    for (struct trip **ptr = trips; ptr < trips + num_trips; ptr++)
    {
        struct trip *trip_ptr = *ptr;
        polyline_buffer_size += trip_ptr->polyline_size * sizeof(struct coordinate);
    }

    int trip_chunk_size = (max_trip_id_length + sizeof(int) * 9);
    int trip_buffer_size = num_trips * trip_chunk_size;
    int file_size = sizeof(num_trips) + sizeof(max_trip_id_length) + trip_buffer_size + polyline_buffer_size;

    /* open file */
    int fd_out = open(path, O_RDWR | O_CREAT, 0644);
    assert(fd_out >= 0);

    ret = ftruncate(fd_out, file_size);
    assert(ret == 0);

    char *mem_begin = (char*) mmap(NULL, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_out, 0);
    assert(mem_begin != NULL);
    char *mem_end = mem_begin + file_size;
    char *mem_ptr = mem_begin;

    int min_chunk_size = (num_trips - 1) / max_num_workers + 1;
    int num_chunks = 0;

    int tab_trip_index[max_num_workers + 1];
    int tab_polyline_index[max_num_workers + 1];
    tab_trip_index[0] = 0;
    tab_polyline_index[0] = 0;

    for (num_chunks = 1; num_chunks <= max_num_workers; num_chunks++)
    {
        tab_trip_index[num_chunks] = tab_trip_index[num_chunks - 1] + min_chunk_size;

        if (tab_trip_index[num_chunks] >= num_trips)
        {
            tab_trip_index[num_chunks] = num_trips;
            break;
        }
    }

#pragma omp parallel for
    for (int i = 0; i < num_chunks; i++)
    {
        int num_coordinates = 0;
        for (struct trip **ptr = &trips[tab_trip_index[i]]; ptr < &trips[tab_trip_index[i + 1]]; ptr++)
        {
            struct trip *trip_ptr = *ptr;
            num_coordinates += trip_ptr->polyline_size;
        }
        tab_polyline_index[i + 1] = num_coordinates;
    }

    for (int i = 1; i <= num_chunks; i++)
    {
        tab_polyline_index[i] += tab_polyline_index[i - 1];
    }

    *(int*) mem_begin = num_trips;
    *(int*) (mem_begin + sizeof(num_trips)) = max_trip_id_length;

    int offset_trip_buffer = sizeof(num_trips) + sizeof(max_train_trip_id_length);
    int offset_polyline_buffer = offset_trip_buffer + trip_buffer_size;

#pragma omp parallel for
    for (int idx = 0; idx < num_chunks; idx++)
    {
        struct trip **ptr = &trips[tab_trip_index[idx]];
        char *mem_ptr = mem_begin + offset_trip_buffer + tab_trip_index[idx] * trip_chunk_size;
        char *polyline_ptr = mem_begin + offset_polyline_buffer + sizeof(struct coordinate) * tab_polyline_index[idx];
        int polyline_index = tab_polyline_index[idx];

        for (int i = tab_trip_index[idx]; i < tab_trip_index[idx + 1]; i++)
        {
            struct trip *trip_ptr = *ptr;
            strcpy(mem_ptr, trip_ptr->trip_id);
            mem_ptr += max_trip_id_length;

            *(int*) mem_ptr = trip_ptr->call_type;
            mem_ptr += sizeof(trip_ptr->call_type);

            *(int*) mem_ptr = trip_ptr->origin_call;
            mem_ptr += sizeof(trip_ptr->origin_call);

            *(int*) mem_ptr = trip_ptr->origin_stand;
            mem_ptr += sizeof(trip_ptr->origin_stand);

            *(int*) mem_ptr = trip_ptr->taxi_id;
            mem_ptr += sizeof(trip_ptr->taxi_id);

            *(int*) mem_ptr = trip_ptr->timestamp;
            mem_ptr += sizeof(trip_ptr->timestamp);

            *(int*) mem_ptr = trip_ptr->day_type;
            mem_ptr += sizeof(trip_ptr->day_type);

            *(int*) mem_ptr = trip_ptr->missing_data;
            mem_ptr += sizeof(trip_ptr->missing_data);

            *(int*) mem_ptr = trip_ptr->polyline_size;
            mem_ptr += sizeof(trip_ptr->polyline_size);

            *(int*) mem_ptr = polyline_index;
            mem_ptr += sizeof(int);

            int polyline_chunk_size = sizeof(struct coordinate) * trip_ptr->polyline_size;
            memcpy(polyline_ptr, trip_ptr->polyline, polyline_chunk_size);

            polyline_index += trip_ptr->polyline_size;
            polyline_ptr += polyline_chunk_size;
            ptr++;
        }
    }

    ret = munmap(mem_begin, file_size);
    assert(ret == 0);
}

int main(int argc, char **argv)
{
    if (argc != 7)
    {
        fprintf(stderr, "Usage: %s meta_csv_file train_csv_file test_csv_file meta_output_file train_output_file test_output_file\n\n", argv[0]);
        return 1;
    }

    max_num_workers = omp_get_max_threads();
    assert(max_num_workers >= 1);

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
    parse_meta_data(argv[1]);
    parse_csv_data(argv[2], train_trips, train_trip_pointers, train_positions, &num_train_trips, &max_train_trip_id_length);
    parse_csv_data(argv[3], test_trips, test_trip_pointers, test_positions, &num_test_trips, &max_test_trip_id_length);

    dump_meta_data(argv[4]);
    dump_trips(argv[5], num_train_trips, max_train_trip_id_length, train_trip_pointers);
    dump_trips(argv[6], num_test_trips, max_test_trip_id_length, test_trip_pointers);

    return 0;
}
