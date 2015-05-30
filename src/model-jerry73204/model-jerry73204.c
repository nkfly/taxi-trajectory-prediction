#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <fcntl.h>
#include <errno.h>
#include <omp.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include "model-jerry73204.h"

#define MAX_NUM_METAS 63
#define MAX_NUM_TRIPS 8388608
#define MAX_NUM_POSITIONS 134217728

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

void load_meta_data(char *path)
{
    /* open file */
    assert(!access(path, F_OK) && !access(path, R_OK));

    int fd_meta = open(path, O_RDONLY);
    assert(fd_meta >= 0);

    struct stat stat_buf;
    assert(!fstat(fd_meta, &stat_buf));

    char *mem_begin = (char*) mmap(NULL, stat_buf.st_size, PROT_READ, MAP_SHARED | MAP_POPULATE, fd_meta, 0);
    assert(mem_begin != NULL);
    char *mem_end = mem_begin + stat_buf.st_size;
    assert(!close(fd_meta));

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
    assert(!munmap(mem_begin, stat_buf.st_size));
}

void load_train_data(char *path)
{
    /* open file */
    assert(!access(path, F_OK) && !access(path, R_OK));

    int fd_train = open(path, O_RDONLY);
    assert(fd_train >= 0);

    struct stat stat_buf;
    assert(!fstat(fd_train, &stat_buf));

    char *mem_begin = (char*) mmap(NULL, stat_buf.st_size, PROT_READ, MAP_SHARED | MAP_POPULATE, fd_train, 0);
    assert(mem_begin != NULL);
    char *mem_end = mem_begin + stat_buf.st_size;
    assert(!close(fd_train));

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
        struct trip *trip_begin = &train_trips[i * num_trips_per_worker];
        struct trip *trip_ptr = trip_begin;
        struct coordinate *position_begin = &train_positions[i * num_positions_per_worker];
        struct coordinate *position_ptr = &train_positions[i * num_positions_per_worker];
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

    assert(!munmap(mem_begin, stat_buf.st_size));

    int trip_begin_index[num_chunks];
    trip_begin_index[0] = 0;
    for (int i = 1; i < num_chunks; i++)
    {
        trip_begin_index[i] = trip_begin_index[i - 1] + trip_count[i - 1];
    }

#pragma omp parallel for
    for (int i = 0; i < num_chunks; i++)
    {
        struct trip *trip_begin = &train_trips[i * num_trips_per_worker];
        struct trip *trip_ptr = trip_begin;

        struct trip **trip_pointers_begin = &train_trip_pointers[trip_begin_index[i]];
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

void load_test_data(char *path)
{
    /* open file */
    assert(!access(path, F_OK) && !access(path, R_OK));

    int fd_test = open(path, O_RDONLY);
    assert(fd_test >= 0);

    struct stat stat_buf;
    assert(!fstat(fd_test, &stat_buf));

    char *mem_begin = (char*) mmap(NULL, stat_buf.st_size, PROT_READ, MAP_SHARED | MAP_POPULATE, fd_test, 0);
    assert(mem_begin != NULL);
    char *mem_end = mem_begin + stat_buf.st_size;
    assert(!close(fd_test));

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
        struct trip *trip_begin = &test_trips[i * num_trips_per_worker];
        struct trip *trip_ptr = trip_begin;
        struct coordinate *position_begin = &test_positions[i * num_positions_per_worker];
        struct coordinate *position_ptr = &test_positions[i * num_positions_per_worker];
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

    assert(!munmap(mem_begin, stat_buf.st_size));

    int trip_begin_index[num_chunks];
    trip_begin_index[0] = 0;
    for (int i = 1; i < num_chunks; i++)
    {
        trip_begin_index[i] = trip_begin_index[i - 1] + trip_count[i - 1];
    }

#pragma omp parallel for
    for (int i = 0; i < num_chunks; i++)
    {
        struct trip *trip_begin = &test_trips[i * num_trips_per_worker];
        struct trip *trip_ptr = trip_begin;

        struct trip **trip_pointers_begin = &test_trip_pointers[trip_begin_index[i]];
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

int main(int argc, char **argv)
{
    assert(argc == 4);

    train_trips = (struct trip*) malloc(MAX_NUM_TRIPS * sizeof(struct trip));
    assert(train_trips != NULL);

    test_trips = (struct trip*) malloc(MAX_NUM_TRIPS * sizeof(struct trip));
    assert(test_trips != NULL);

    train_positions = (struct coordinate*) malloc(MAX_NUM_POSITIONS * sizeof(struct coordinate));
    assert(train_positions != NULL);

    test_positions = (struct coordinate*) malloc(MAX_NUM_POSITIONS * sizeof(struct coordinate));
    assert(test_positions != NULL);

    load_meta_data(argv[1]);
    load_train_data(argv[2]);
    load_test_data(argv[3]);

    return 0;
}
