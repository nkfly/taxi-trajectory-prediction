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

#include "cluster.h"

#define MAX_NUM_METAS 63
#define MAX_NUM_TRIPS 8388608
#define MAX_NUM_POSITIONS 134217728
#define COMPARED_POLYLINE_LENTH 16
#define MAX_NUM_PREDICTIONS 320
#define THRESHOLD 0.5
#define DOUBLE_INFINITY ((1.0 / 0.0))

#define RADIUS 6371
#define RAD_DEG_RATIO (3.14159265358979323846264338 / 180)

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

int max_num_workers;

double dist(struct coordinate *p1, struct coordinate *p2)
{
    double th1 = p1->longitude;
    double ph1 = p1->latitude;

    double th2 = p2->longitude;
    double ph2 = p2->latitude;

    double dx, dy, dz;
    ph1 -= ph2;
    ph1 *= RAD_DEG_RATIO;
    th1 *= RAD_DEG_RATIO;
    th2 *= RAD_DEG_RATIO;

    dz = sin(th1) - sin(th2);
    dx = cos(ph1) * cos(th1) - cos(th2);
    dy = sin(ph1) * cos(th1);
    return asin(sqrt(dx * dx + dy * dy + dz * dz) / 2) * 2 * RADIUS;
}

/* find distance b/w two coordinate */
double distance_coordinate(struct coordinate *line_left, struct coordinate *line_right)
{
    double longitude_diff = line_left->longitude - line_right->longitude;
    double latitude_diff = line_left->latitude - line_right->latitude;
    return sqrt(longitude_diff * longitude_diff + latitude_diff * latitude_diff);
}

/* find the norm b/w two polylines */
double distance_polyline(struct coordinate *line_left, struct coordinate *line_right, int n)
{
    assert(n > 0);
    double norm = 0.0;

    for (int i = 0; i < n; i++)
    {
        norm += dist(line_left, line_right);
        line_left++;
        line_right++;
    }
    return norm;
}

/* find minimum norm b/w two trips */
double distance_trip(struct coordinate *polyline_left, int size_left, struct coordinate *polyline_right, int size_right)
{
    if (size_left > size_right)
    {
        SWAP(&size_left, &size_right);
        SWAP_POINTER(&polyline_left, &polyline_right);
    }

    struct coordinate *left_polyline_ptr = polyline_left;
    struct coordinate *right_polyline_ptr = polyline_right;
    double min_distance = DOUBLE_INFINITY; /* infinity */

    for (int i = 0; i <= size_right - size_left; i++)
    {
        double distance = distance_polyline(right_polyline_ptr, left_polyline_ptr, size_left);
        min_distance = ( distance < min_distance ? distance : min_distance );
        right_polyline_ptr++;
    }

    return min_distance / size_left;
}

void cluster(char *path)
{
    int ret;
    /* open file */
    ret = access(path, F_OK) || access(path, R_OK);
    assert(ret == 0);

    int fd = open(path, O_RDONLY);
    assert(fd >= 0);

    struct stat stat_buf;
    ret = fstat(fd, &stat_buf);
    assert(ret == 0);

    char *mem_begin = (char*) mmap(NULL, stat_buf.st_size, PROT_READ, MAP_SHARED | MAP_POPULATE, fd, 0);
    assert(mem_begin != NULL);
    char *mem_end = mem_begin + stat_buf.st_size;
    ret = close(fd);
    assert(ret == 0);

    int num_trips = *(int*) mem_begin;
    int max_trip_id_length = *(int*) (mem_begin + sizeof(int));
    int struct_size = max_trip_id_length + sizeof(int) * 9;

    char *mem_trips = mem_begin + sizeof(int) * 2;
    char *mem_coordinates = mem_begin + sizeof(int) * 2 + num_trips * struct_size;

    int *flags = (int*) calloc(num_trips, sizeof(int));
    assert(flags != NULL);
    int *center_indices = (int*) calloc(num_trips, sizeof(int));
    assert(center_indices != NULL);

    int num_chunks;
    int min_chunk_size = (num_trips - 1) / max_num_workers + 1;
    int trip_indices[max_num_workers + 1];
    trip_indices[0] = 0;

    for (num_chunks = 1; num_chunks <= num_trips; num_chunks++)
    {
        trip_indices[num_chunks] = trip_indices[num_chunks - 1] + min_chunk_size;
        if (trip_indices[num_chunks] >= num_trips)
        {
            trip_indices[num_chunks] = num_trips;
            break;
        }
    }

    int *indices = (int *) malloc(sizeof(int) * num_trips);
    assert(indices != NULL);

    int num_trips_left = 0;
    int index_count[num_chunks];
    memset(index_count, 0, sizeof(int) * num_chunks);

#pragma omp parallel for reduction(+: num_trips_left)
    for (int idx = 0; idx < num_chunks; idx++)
    {
        int begin = trip_indices[idx];
        int end = trip_indices[idx + 1];
        char *ptr = mem_begin + sizeof(int) * 2 + struct_size * begin;
        int *index_ptr = &indices[begin];

        for (int i = begin; i < end; i++)
        {
            int polyline_size = *(int*) (ptr + max_trip_id_length + sizeof(int) * 7);
            if (polyline_size != 0)
            {
                *index_ptr = i;
                index_ptr++;
            }
            else
                flags[i] = -1;
            ptr += struct_size;
        }
        index_count[idx] = index_ptr - &indices[begin];
        num_trips_left += index_count[idx];
    }

    int num_clusters = 0;

    while (1)
    {
        /* find next center polyline */
        int center_index;
        int j;
        for (j = 0; j < num_chunks; j++)
        {
            if (index_count[j] > 0)
            {
                center_index = center_indices[num_clusters] = indices[trip_indices[j]];
                break;
            }
        }

        if (j == num_chunks)
            break;

        int center_polyline_size = *(int*) (mem_trips + struct_size * center_index + max_trip_id_length + sizeof(int) * 7);
        int center_polyline_index = *(int*) (mem_trips + struct_size * center_index + max_trip_id_length + sizeof(int) * 8);
        struct coordinate *center_polyline = (struct coordinate*) (mem_coordinates + sizeof(struct coordinate) * center_polyline_index);

#pragma omp parallel for
        for (int idx = 0; idx < num_chunks; idx++)
        {
            int *ptr_begin = &indices[trip_indices[idx]];
            int *ptr_end = ptr_begin + index_count[idx];

            int *ptr_next_index = ptr_begin;

            for (int *index_ptr = ptr_begin; index_ptr < ptr_end; index_ptr++)
            {
                int index = *index_ptr;
                int polyline_size = *(int*) (mem_trips + struct_size * index + max_trip_id_length + sizeof(int) * 7);
                int polyline_index = *(int*) (mem_trips + struct_size * index + max_trip_id_length + sizeof(int) * 8);
                struct coordinate *polyline = (struct coordinate*) (mem_coordinates + sizeof(struct coordinate) * polyline_index);

                double distance = distance_trip(center_polyline, center_polyline_size, polyline, polyline_size);
                if (distance <= THRESHOLD)
                    flags[index] = num_clusters;
                else
                {
                    *ptr_next_index = index;
                    ptr_next_index++;
                }
            }
            index_count[idx] = ptr_next_index - ptr_begin;
        }

        num_clusters++;
    }

    write(1, &num_clusters, sizeof(int));
    write(1, center_indices, sizeof(int) * num_clusters);
    write(1, flags, sizeof(int) * num_trips);
}

int main(int argc, char **argv)
{
    if (argc != 2)
    {
        fprintf(stderr, "Usage: %s train_data\n\n", argv[0]);
        return 1;
    }

    max_num_workers = omp_get_max_threads();
    assert(max_num_workers > 1);

    /* parse dataset */
    cluster(argv[1]);

    return 0;
}
