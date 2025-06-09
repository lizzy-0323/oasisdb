#include "hnsw_c_api.h"
#include "../../index/hnswlib/hnswlib.h"
#include "../../index/hnswlib/space_l2.h"
#include <memory>

struct HNSWIndex {
  std::unique_ptr<hnswlib::SpaceInterface<float>> space;
  std::unique_ptr<hnswlib::HierarchicalNSW<float>> alg;
  size_t dim;
};

HNSWIndex *hnsw_new(size_t dim, size_t max_elements, size_t M,
                    size_t ef_construction, char stype) {
  auto index = new HNSWIndex();
  hnswlib::SpaceInterface<float> *space;
  index->dim = dim;
  if (stype == 'l') {
    space = new hnswlib::L2Space(dim);
  } else if (stype == 'i') {
    space = new hnswlib::InnerProductSpace(dim);
  } else {
    return nullptr;
  }
  index->space = std::unique_ptr<hnswlib::SpaceInterface<float>>(space);
  index->alg = std::make_unique<hnswlib::HierarchicalNSW<float>>(
      index->space.get(), max_elements, M, ef_construction);
  return index;
}

void hnsw_free(HNSWIndex *index) {
  if (index) {
    delete index;
  }
}

int hnsw_add_point(HNSWIndex *index, const float *point, size_t id) {
  try {
    index->alg->addPoint(point, id);
    return 0;
  } catch (...) {
    return -1;
  }
}

void hnsw_search_knn(HNSWIndex *index, const float *query, size_t k,
                     size_t *labels, float *distances) {
  auto results = index->alg->searchKnn(query, k);
  size_t i = 0;
  while (!results.empty()) {
    auto &result = results.top();
    labels[i] = result.second;
    distances[i] = result.first;
    results.pop();
    i++;
  }
}

void hnsw_set_ef(HNSWIndex *index, size_t ef) {
  if (index && index->alg) {
    index->alg->setEf(ef);
  }
}

int hnsw_save_index(HNSWIndex *index, const char *path) {
  try {
    index->alg->saveIndex(path);
    return 0;
  } catch (...) {
    return -1;
  }
}

HNSWIndex *hnsw_load_index(const char *path, size_t dim, const char spaceType) {
  auto index = new HNSWIndex();
  index->dim = dim;
  hnswlib::SpaceInterface<float> *space;
  if (spaceType == 'l') {
    space = new hnswlib::L2Space(dim);
  } else if (spaceType == 'i') {
    space = new hnswlib::InnerProductSpace(dim);
  } else {
    return nullptr;
  }
  index->space = std::unique_ptr<hnswlib::SpaceInterface<float>>(space);
  index->alg = std::unique_ptr<hnswlib::HierarchicalNSW<float>>(
      new hnswlib::HierarchicalNSW<float>(space, std::string(path), false, 0));
  return index;
}

int hnsw_mark_deleted(HNSWIndex *index, size_t label) {
  try {
    index->alg->markDelete(label);
    return 0;
  } catch (...) {
    return -1;
  }
}

int get_data_by_label(HNSWIndex *index, size_t label, float *out_data) {
  try {
    auto data = index->alg->getDataByLabel<float>(label);
    if (data.empty()) {
      return -1; // label not found
    }
    std::vector<float> *vec = new std::vector<float>(data.begin(), data.end());
    size_t size = vec->size();
    for (size_t i = 0; i < size; i++) {
      out_data[i] = (*vec)[i];
    }
    delete vec;
    return 0;
  } catch (...) {
    return -1;
  }
}

int get_max_elements(HNSWIndex *index) { return index->alg->getMaxElements(); }

int get_current_element_count(HNSWIndex *index) {
  return index->alg->getCurrentElementCount();
}

int get_deleted_count(HNSWIndex *index) {
  return index->alg->getDeletedCount();
}

float get_avg_hops(HNSWIndex *index) { return index->alg->getAvgHops(); }

float get_avg_dist_computations(HNSWIndex *index) {
  return index->alg->getAvgDistComputations();
}

int get_query_count(HNSWIndex *index) { return index->alg->getQueryCount(); }
