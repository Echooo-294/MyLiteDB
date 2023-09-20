#include "executor.h"

#include <iostream>

#include "optimizer.h"
#include "storage/metadata.h"
#include "trx.h"
#include "util.h"

using namespace hsql;

namespace litedb {

void Executor::init() { opTree_ = generateOperator(planTree_); }

bool Executor::exec() { return opTree_->exec(); }

BaseOperator* Executor::generateOperator(Plan* plan) {
  BaseOperator* op = nullptr;
  BaseOperator* next = nullptr;

  // DFS 递归调用，从底端的叶子节点开始往上组织算子
  if (plan->next != nullptr) next = generateOperator(plan->next);

  // 根据执行计划类型创建相应的算子
  switch (plan->plan_type) {
    case kCreate:
      op = new CreateOperator(plan, next);
      break;
    case kDrop:
      op = new DropOperator(plan, next);
      break;
    case kInsert:
      op = new InsertOperator(plan, next);
      break;
    case kUpdate:
      op = new UpdateOperator(plan, next);
      break;
    case kDelete:
      op = new DeleteOperator(plan, next);
      break;
    case kSelect:
      op = new SelectOperator(plan, next);
      break;
    case kScan: {
      ScanPlan* scan_plan = static_cast<ScanPlan*>(plan);
      if (scan_plan->type == kSeqScan) op = new SeqScanOperator(plan, next);
      break;
    }
    case kFilter:
      op = new FilterOperator(plan, next);
      break;
    case kTrx:
      op = new TrxOperator(plan, next);
      break;
    case kShow:
      op = new ShowOperator(plan, next);
      break;
    default:
      std::cout << "[LiteDB-Error]  Not support plan node "
                << PlanTypeToString(plan->plan_type);
      break;
  }

  return op;
}

bool CreateOperator::exec(TupleIter** iter) {
  CreatePlan* plan = static_cast<CreatePlan*>(plan_);

  if (plan->type == kCreateTable) {
    Table* table = new Table(plan->schema, plan->name, plan->columns);
    if (g_meta_data.insertTable(table)) {
      delete table;
      if (plan->if_not_exists) {
        std::cout << "[LiteDB-Info]  Table "
                  << TableNameToString(plan->schema, plan->name)
                  << " already existed.\r\n";
        return false;
      } else {
        std::cout << "[LiteDB-Error]  Table "
                  << TableNameToString(plan->schema, plan->name)
                  << " already existed.\r\n";
        return true;
      }
    }

    if (g_transaction.inTransaction()) {
      char* schema = new char[strlen(plan->schema)];
      char* table_name = new char[strlen(plan->name)];
      strcpy(schema, plan->schema);
      strcpy(table_name, plan->name);
      g_transaction.addCreateTableUndo(schema, table_name);
    }

    std::cout << "[LiteDB-Info]  Create table successfully.\r\n";
    return false;
  } else if (plan->type == kCreateIndex) {
    Table* table = g_meta_data.getTable(plan->schema, plan->name);
    if (table == nullptr) {
      std::cout << "[LiteDB-Error]  Table "
                << TableNameToString(plan->schema, plan->name)
                << " did not exist.\r\n";
      return true;
    }

    Index* index = table->getIndex(plan->index_name);
    if (index != nullptr) {
      if (plan->if_not_exists) {
        std::cout << "[LiteDB-Info]  Index " << plan->index_name
                  << " already existed.\r\n";
        return false;
      } else {
        std::cout << "[LiteDB-Error]  Index " << plan->index_name
                  << " already existed.\r\n";
        return true;
      }
    }

    index = new Index();
    index->name = plan->index_name;
    index->columns = *plan->index_columns;
    table->addIndex(index);

    if (g_transaction.inTransaction()) {
      char* schema = new char[strlen(plan->schema)];
      char* table_name = new char[strlen(plan->name)];
      char* index_name = new char[strlen(plan->index_name)];
      strcpy(schema, plan->schema);
      strcpy(table_name, plan->name);
      strcpy(index_name, plan->index_name);
      g_transaction.addCreateIndexUndo(schema, table_name, index_name);
    }

    std::cout << "[LiteDB-Info]  Create index successfully.\r\n";
  } else {
    std::cout << "[LiteDB-Error]  Invalid 'create' statement.\r\n";
    return true;
  }

  return false;
}

bool DropOperator::exec(TupleIter** iter) {
  DropPlan* plan = static_cast<DropPlan*>(plan_);
  if (plan->type == kDropSchema) {
    std::vector<Table*> tables;
    if (g_meta_data.dropSchema(plan->schema, &tables)) {
      if (plan->if_exists) {
        std::cout << "[LiteDB-Info]  Schema " << plan->schema
                  << " did not exist.\r\n";
        return false;
      } else {
        std::cout << "[LiteDB-Error]  Schema " << plan->schema
                  << " did not exist.\r\n";
        return true;
      }
    }

    if (g_transaction.inTransaction())
      g_transaction.addDropSchemaUndo(&tables);
    else
      for (const auto table : tables) delete table;

    std::cout << "[LiteDB-Info]  Drop schema successfully.\r\n";
    return false;
  } else if (plan->type == kDropTable) {
    Table* table;
    if (g_meta_data.dropTable(plan->schema, plan->name, &table)) {
      if (plan->if_exists) {
        std::cout << "[LiteDB-Info]  Table "
                  << TableNameToString(plan->schema, plan->name)
                  << " did not exist.\r\n";
        return false;
      } else {
        std::cout << "[LiteDB-Error]  Table "
                  << TableNameToString(plan->schema, plan->name)
                  << " did not exist.\r\n";
        return true;
      }
    }

    if (g_transaction.inTransaction())
      g_transaction.addDropTableUndo(table);
    else
      delete table;

    std::cout << "[LiteDB-Info]  Drop table successfully.\r\n";
    return false;
  } else if (plan->type == kDropIndex) {
    Index* index;
    if (g_meta_data.dropIndex(plan->schema, plan->name, plan->index_name,
                              &index)) {
      if (plan->if_exists) {
        std::cout << "[LiteDB-Info]  Index " << plan->index_name
                  << " did not exist.\r\n";
        return false;
      } else {
        std::cout << "[LiteDB-Error]  Index " << plan->index_name
                  << " did not exist.\r\n";
        return true;
      }
    }

    if (g_transaction.inTransaction()) {
      char* schema = new char[strlen(plan->schema)];
      char* name = new char[strlen(plan->name)];
      strcpy(schema, plan->schema);
      strcpy(name, plan->name);
      g_transaction.addDropIndexUndo(schema, name, index);
    } else {
      delete index;
    }

    std::cout << "[LiteDB-Info]  Drop index successfully.\r\n";
    return false;
  } else {
    std::cout << "[LiteDB-Error]  Invalid 'Drop' statement.\r\n";
    return true;
  }

  return false;
}

bool InsertOperator::exec(TupleIter** iter) {
  InsertPlan* plan = static_cast<InsertPlan*>(plan_);
  TableStore* table_store = plan->table->getTableStore();
  if (table_store->insertTuple(plan->values)) return true;

  std::cout << "[LiteDB-Info]  Insert tuple successfully.\r\n";
  return false;
}

bool UpdateOperator::exec(TupleIter** iter) {
  UpdatePlan* update = static_cast<UpdatePlan*>(plan_);
  Table* table = update->table;
  TableStore* table_store = table->getTableStore();
  int upd_cnt = 0;

  while (true) {
    TupleIter* tup_iter = nullptr;
    if (next_->exec(&tup_iter)) return true;

    if (tup_iter == nullptr) {
      break;
    } else {
      table_store->updateTuple(tup_iter->tup, update->idxs, update->values);
      upd_cnt++;
    }
  }

  std::cout << "[LiteDB-Info]  Update " << upd_cnt
            << " tuple successfully.\r\n";
  return false;
}

bool DeleteOperator::exec(TupleIter** iter) {
  Table* table = static_cast<DeletePlan*>(plan_)->table;
  TableStore* table_store = table->getTableStore();
  int del_cnt = 0;

  while (true) {
    TupleIter* tup_iter = nullptr;
    if (next_->exec(&tup_iter)) return true;

    if (tup_iter == nullptr) {
      break;
    } else {
      table_store->deleteTuple(tup_iter->tup);
      del_cnt++;
    }
  }

  std::cout << "[LiteDB-Info]  Delete " << del_cnt
            << " tuple successfully.\r\n";
  return false;
}

bool SelectOperator::exec(TupleIter** iter) {
  SelectPlan* plan = static_cast<SelectPlan*>(plan_);
  std::vector<std::vector<Expr*>> tuples;
  while (true) {
    TupleIter* tup_iter = nullptr;
    if (next_->exec(&tup_iter)) return true;

    if (tup_iter == nullptr)
      break;
    else
      tuples.emplace_back(tup_iter->values);
  }

  PrintTuples(plan->out_cols, plan->col_ids, tuples);
  return false;
}

bool SeqScanOperator::exec(TupleIter** iter) {
  ScanPlan* plan = static_cast<ScanPlan*>(plan_);
  TableStore* table_store = plan->table->getTableStore();
  Tuple* tup = nullptr;

  if (finish_) return false;

  if (next_tuple_ == nullptr)
    tup = table_store->seqScan(nullptr);
  else
    tup = next_tuple_;

  if (tup == nullptr) {
    *iter = nullptr;
    return false;
  }

  TupleIter* tup_iter = new TupleIter(tup);
  table_store->parseTuple(tup, tup_iter->values);
  tuples_.emplace_back(tup_iter);
  *iter = tup_iter;

  next_tuple_ = table_store->seqScan(tup);
  if (next_tuple_ == nullptr) finish_ = true;

  return false;
}

bool FilterOperator::exec(TupleIter** iter) {
  *iter = nullptr;
  while (true) {
    TupleIter* tup_iter = nullptr;
    if (next_->exec(&tup_iter)) return true;

    if (tup_iter == nullptr) break;

    if (execEqualExpr(tup_iter)) {
      *iter = tup_iter;
      break;
    }
  }

  return false;
}

bool FilterOperator::execEqualExpr(TupleIter* iter) {
  FilterPlan* filter = static_cast<FilterPlan*>(plan_);
  Expr* val = filter->val;
  size_t col_id = filter->idx;

  Expr* col_val = iter->values[col_id];
  if (col_val->type != val->type) return false;

  if (col_val->type == kExprLiteralInt) return (col_val->ival == val->ival);

  if (col_val->type == kExprLiteralFloat) return (col_val->ival == val->ival);

  if (col_val->type == kExprLiteralString)
    return (strcmp(col_val->name, val->name) == 0);

  return false;
}

bool TrxOperator::exec(TupleIter** iter) {
  TrxPlan* plan = static_cast<TrxPlan*>(plan_);
  switch (plan->command) {
    case kBeginTransaction: {
      if (g_transaction.inTransaction()) {
        std::cout << "[LiteDB-Error]  Already in transaction\r\n";
        return true;
      }
      g_transaction.begin();
      std::cout << "[LiteDB-Info]  Start transaction\r\n";
      break;
    }
    case kCommitTransaction: {
      if (!g_transaction.inTransaction()) {
        std::cout << "[LiteDB-Error]  Not in transaction\r\n";
        return true;
      }
      g_transaction.commit();
      std::cout << "[LiteDB-Info]  Commit transaction\r\n";
      break;
    }
    case kRollbackTransaction: {
      if (!g_transaction.inTransaction()) {
        std::cout << "[LiteDB-Error]  Not in transaction\r\n";
        return true;
      }
      g_transaction.rollback();
      std::cout << "[LiteDB-Info]  Rollback transaction\r\n";
      break;
    }
    default:
      break;
  }

  return false;
}

bool ShowOperator::exec(TupleIter** iter) {
  ShowPlan* show_plan = static_cast<ShowPlan*>(plan_);
  if (show_plan->type == kShowTables) {
    std::vector<Table*> tables;
    g_meta_data.getAllTables(&tables);

    std::cout << "# Table List:\r\n";
    for (auto table : tables)
      std::cout << TableNameToString(table->schema(), table->name()) << "\r\n";
  } else if (show_plan->type == kShowColumns) {
    Table* table = g_meta_data.getTable(show_plan->schema, show_plan->name);
    if (table == nullptr) {
      std::cout << "[LiteDB-Error]  Failed to find table "
                << TableNameToString(show_plan->schema, show_plan->name)
                << "\r\n";
      return true;
    }
    std::cout << "# Columns in "
              << TableNameToString(show_plan->schema, show_plan->name) << ":"
              << "\r\n";
    for (auto col_def : *table->columns())
      if (col_def->type.data_type == DataType::CHAR ||
          col_def->type.data_type == DataType::VARCHAR)
        std::cout << col_def->name << "\t"
                  << DataTypeToString(col_def->type.data_type) << "("
                  << col_def->type.length << ")"
                  << "\r\n";
      else
        std::cout << col_def->name << "\t"
                  << DataTypeToString(col_def->type.data_type) << "\r\n";
  } else {
    std::cout << "[LiteDB-Error]  Invalid 'Show' statement.\r\n";
    return true;
  }

  return false;
}

}  // namespace litedb