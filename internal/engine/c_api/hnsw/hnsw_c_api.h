#ifndef HNSW_C_API_H
#define HNSW_C_API_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>
#include <stdlib.h>

// Opaque type for HNSW index
typedef struct HNSWIndex HNSWIndex;

// Create a new HNSW index
HNSWIndex *hnsw_new(size_t dim, size_t max_elements, size_t M,
                    size_t ef_construction, char stype);

// Free the HNSW index
void hnsw_free(HNSWIndex *index);

// Add a point to the index
int hnsw_add_point(HNSWIndex *index, const float *point, size_t id);

// Search for nearest neighbors
void hnsw_search_knn(HNSWIndex *index, const float *query, size_t k,
                     size_t *labels, float *distances);

// Set ef parameter for search
void hnsw_set_ef(HNSWIndex *index, size_t ef);

// Save index to file
int hnsw_save_index(HNSWIndex *index, const char *path);

// Load index from file
HNSWIndex *hnsw_load_index(const char *path, size_t dim, const char spaceType);

// Mark an element as deleted
int hnsw_mark_deleted(HNSWIndex *index, size_t label);

// Returns 0 on success, -1 if label not found or on error
int get_data_by_label(HNSWIndex *index, size_t label, float *data);

// Get max elements
int get_max_elements(HNSWIndex *index);

// Get current element count
int get_current_element_count(HNSWIndex *index);

// Get deleted count
int get_deleted_count(HNSWIndex *index);

// Get average hops for queries
float get_avg_hops(HNSWIndex *index);

// Get metric distance computations
float get_avg_dist_computations(HNSWIndex *index);

// Get query count
int get_query_count(HNSWIndex *index);

#ifdef __cplusplus
}
#endif

#endif // HNSW_C_API_H