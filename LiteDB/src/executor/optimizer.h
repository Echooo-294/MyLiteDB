#pragma once

#include "sql/statements.h"
#include "storage/metadata.h"

using namespace hsql;

namespace litedb {
enum PlanType {
  kCreate,
  kDrop,
  kInsert,
  kUpdate,
  kDelete,
  kSelect,
  kScan,
  kProjection,
  kFilter,
  kSort,
  kLimit,
  kTrx,
  kShow
};

struct Plan {
  Plan(PlanType t) : plan_type(t), next(nullptr) {}
  ~Plan() {
    delete next;
    next = nullptr;
  }

  PlanType plan_type;
  Plan* next;
};

struct CreatePlan : public Plan {
  CreatePlan(CreateType t) : Plan(kCreate), type(t) {}
  CreateType type;
  bool if_not_exists;
  char* schema;
  char* name;
  char* index_name;
  std::vector<ColumnDefinition*>* index_columns;
  std::vector<ColumnDefinition*>* columns;
};

struct DropPlan : public Plan {
  DropPlan() : Plan(kDrop) {}
  DropType type;
  bool if_exists;
  char* schema;
  char* name;
  char* index_name;
};

struct InsertPlan : public Plan {
  InsertPlan() : Plan(kInsert) {}
  InsertType type;
  Table* table;
  std::vector<Expr*>* values;
};

struct UpdatePlan : public Plan {
  UpdatePlan() : Plan(kUpdate) {}
  Table* table;
  std::vector<Expr*> values;
  std::vector<size_t> idxs;
};

struct DeletePlan : public Plan {
  DeletePlan() : Plan(kDelete) {}
  Table* table;
};

struct SelectPlan : public Plan {
  SelectPlan() : Plan(kSelect) {}
  Table* table;
  std::vector<ColumnDefinition*> out_cols;
  std::vector<size_t> col_ids;
};

enum ScanType { kSeqScan, kIndexScan };

struct ScanPlan : public Plan {
  ScanPlan() : Plan(kScan) {}
  ScanType type;
  Table* table;
};

struct FilterPlan : public Plan {
  FilterPlan() : Plan(kFilter), idx(0), val(nullptr) {}
  size_t idx;
  Expr* val;
};

struct SortPlan : public Plan {
  SortPlan() : Plan(kSort) {}
  Table* table;
  std::vector<OrderDescription*>* order;
};

struct LimitPlan : public Plan {
  LimitPlan() : Plan(kLimit) {}
  uint64_t offset;
  uint64_t limit;
};

struct TrxPlan : public Plan {
  TrxPlan() : Plan(kTrx) {}
  TransactionCommand command;
};

struct ShowPlan : public Plan {
  ShowPlan() : Plan(kShow) {}
  ShowType type;
  char* schema;
  char* name;
};

class Optimizer {
 public:
  Optimizer() {}

  Plan* createPlanTree(const SQLStatement* stmt);

 private:
  Plan* createSelectPlanTree(const SelectStatement* stmt);

  Plan* createInsertPlanTree(const InsertStatement* stmt);

  Plan* createUpdatePlanTree(const UpdateStatement* stmt);

  Plan* createDeletePlanTree(const DeleteStatement* stmt);

  Plan* createCreatePlanTree(const CreateStatement* stmt);

  Plan* createDropPlanTree(const DropStatement* stmt);

  Plan* createTrxPlanTree(const TransactionStatement* stmt);

  Plan* createShowPlanTree(const ShowStatement* stmt);

  Plan* createFilterPlan(std::vector<ColumnDefinition*>* columns, Expr* where);
};

}  // namespace litedb