#define MAX_META_NAME_SIZE 32
#define MAX_POLYLINE_SIZE 32

#define CALL_TYPE_A 0
#define CALL_TYPE_B 1
#define CALL_TYPE_C 2

#define DAY_TYPE_A 0

struct coordinate
{
    double longitude;
    double latitude;
};

struct meta
{
    char name[MAX_META_NAME_SIZE];
    struct coordinate position;
};

struct trip
{
    unsigned long long int trip_id;
    int call_type;
    int origin_call;
    int origin_stand;
    int taxi_id;
    int timestamp;
    int day_type;
    int missing_data;
    int polyline_size;
    struct coordinate *polyline;
};
