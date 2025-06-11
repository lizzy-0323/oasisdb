#include "ivf_c_api.h"
#include <faiss/AutoTune.h>
#include <faiss/IndexFlat.h>
#include <faiss/IndexIVFFlat.h>
#include <faiss/index_io.h>
#include <iostream>
#include <memory>

struct IVFIndex {
  std::unique_ptr<faiss::IndexIVFFlat> index;
  uint32_t Dimension;
};

IVFIndex *ivf_create_index(uint32_t dimension, uint32_t nlist) {
  try {
    auto index = new IVFIndex();
    index->Dimension = dimension;

    auto quantizer = new faiss::IndexFlatL2(dimension);
    index->index.reset(new faiss::IndexIVFFlat(quantizer, dimension, nlist));

    return index;
  } catch (const std::bad_alloc &) {
    return nullptr;
  } catch (...) {
    return nullptr;
  }
}

void ivf_free_index(IVFIndex *index) {
  if (index) {
    delete index;
  }
}

ivf_error_t ivf_train(IVFIndex *index, uint32_t n, const float *vectors) {
  if (!index) {
    return IVF_ERROR_INVALID_ARGUMENT;
  }
  if (!index->index) {
    return IVF_ERROR_INDEX_BUILD;
  }
  if (!vectors || n == 0) {
    return IVF_ERROR_INVALID_ARGUMENT;
  }

  try {
    // Check if the index is already trained
    if (index->index->is_trained) {
      return IVF_SUCCESS; // Already trained, no need to train again
    }

    // Check if we have enough data to train
    if (n < index->index->nlist) {
      return IVF_ERROR_INVALID_ARGUMENT; // Need at least nlist training vectors
    }

    // Train the quantizer first
    faiss::IndexFlatL2 *quantizer =
        dynamic_cast<faiss::IndexFlatL2 *>(index->index->quantizer);
    if (!quantizer) {
      return IVF_ERROR_INDEX_BUILD;
    }

    // Train the IVF index
    index->index->train(n, vectors);

    if (!index->index->is_trained) {
      return IVF_ERROR_INDEX_BUILD; // Training failed
    }

    return IVF_SUCCESS;
  } catch (const std::bad_alloc &) {
    return IVF_ERROR_MEMORY;
  } catch (const faiss::FaissException &e) {
    std::cerr << "FaissException: " << e.what() << std::endl;
    return IVF_ERROR_INDEX_BUILD;
  } catch (...) {
    return IVF_ERROR_COMMON;
  }
}

ivf_error_t ivf_add(IVFIndex *index, uint32_t n, const float *vectors,
                    const int64_t *ids) {
  if (!index || !vectors || !ids || n == 0) {
    return IVF_ERROR_INVALID_ARGUMENT;
  }

  try {
    index->index->add_with_ids(n, vectors, ids);
    return IVF_SUCCESS;
  } catch (...) {
    return IVF_ERROR_COMMON;
  }
}

ivf_error_t ivf_search(IVFIndex *index, uint32_t n, const float *queries,
                       uint32_t k, uint32_t nprobe, int64_t *labels,
                       float *distances) {
  if (!index || !queries || !labels || !distances || n == 0 || k == 0) {
    return IVF_ERROR_INVALID_ARGUMENT;
  }
  if (!index->index->is_trained) {
    return IVF_ERROR_NOT_TRAINED;
  }

  try {
    index->index->nprobe = nprobe;
    index->index->search(n, queries, k, distances, labels);
    return IVF_SUCCESS;
  } catch (...) {
    return IVF_ERROR_COMMON;
  }
}

ivf_error_t ivf_remove(IVFIndex *index, uint32_t n, const int64_t *ids) {
  if (!index || !ids || n == 0) {
    return IVF_ERROR_INVALID_ARGUMENT;
  }

  try {
    index->index->remove_ids(faiss::IDSelectorBatch(n, ids));
    return IVF_SUCCESS;
  } catch (...) {
    return IVF_ERROR_COMMON;
  }
}

uint64_t ivf_size(IVFIndex *index) { return index ? index->index->ntotal : 0; }

ivf_error_t ivf_save(IVFIndex *index, const char *filename) {
  if (!index || !filename) {
    return IVF_ERROR_INVALID_ARGUMENT;
  }

  try {
    faiss::write_index(index->index.get(), filename);
    return IVF_SUCCESS;
  } catch (...) {
    return IVF_ERROR_COMMON;
  }
}

IVFIndex *ivf_load(const char *filename) {
  if (!filename) {
    return nullptr;
  }

  try {
    auto idx = new IVFIndex();
    idx->index.reset(
        dynamic_cast<faiss::IndexIVFFlat *>(faiss::read_index(filename)));
    if (!idx->index) {
      delete idx;
      return nullptr;
    }
    idx->Dimension = idx->index->d;
    return idx;
  } catch (...) {
    return nullptr;
  }
}