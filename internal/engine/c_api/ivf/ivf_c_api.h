#ifndef IVF_C_API_H
#define IVF_C_API_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stdint.h>

// IVF Index handle
typedef struct IVFIndex IVFIndex;

// Error codes
typedef enum {
  IVF_SUCCESS = 0,
  IVF_ERROR_INVALID_ARGUMENT = 1,
  IVF_ERROR_MEMORY = 2,
  IVF_ERROR_INDEX_BUILD = 3,
  IVF_ERROR_NOT_FOUND = 4,
  IVF_ERROR_DIMENSION_MISMATCH = 5,
  IVF_ERROR_NOT_TRAINED = 6,
  IVF_ERROR_COMMON = 7
} ivf_error_t;

// Create a new IVF index
IVFIndex *ivf_create_index(uint32_t dimension, uint32_t nlist);

// Free an IVF index
void ivf_free_index(IVFIndex *index);

// Train the index with training vectors
ivf_error_t ivf_train(IVFIndex *index, uint32_t n, const float *vectors);

// Add vectors to the index
ivf_error_t ivf_add(IVFIndex *index, uint32_t n, const float *vectors,
                    const int64_t *ids);

// Search for nearest neighbors
ivf_error_t ivf_search(IVFIndex *index, uint32_t n, const float *queries,
                       uint32_t k, uint32_t nprobe, int64_t *labels,
                       float *distances);

// Remove vectors from the index
ivf_error_t ivf_remove(IVFIndex *index, uint32_t n, const int64_t *ids);

// Get the size of the index (number of vectors)
uint64_t ivf_size(IVFIndex *index);

// Save the index to a file
ivf_error_t ivf_save(IVFIndex *index, const char *filename);

// Load the index from a file
IVFIndex *ivf_load(const char *filename);

#ifdef __cplusplus
}
#endif

#endif // OASISDB_IVF_C_API_H
