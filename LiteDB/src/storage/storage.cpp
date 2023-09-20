#include "storage.h"

#include <cstdint>
#include <cstring>
#include <iostream>

#include "sql/ColumnType.h"
#include "sql/Expr.h"
#include "trx.h"
#include "util.h"

using namespace hsql;

namespace litedb {

TableStore::TableStore(std::vector<ColumnDefinition*>* columns)
    : col_num_(columns->size()), tuple_size_(0), columns_(columns) {
  col_offset_.push_back(0);

  // 计算列打印时占用的空间
  for (auto col : *columns) {
    tuple_size_ += ColumnTypeSize(col->type);
    col_offset_.emplace_back(tuple_size_);
  }

  // NULL 也要保留空间
  tuple_size_ += col_num_;

  // 给表头保留空间
  tuple_size_ += TUPLE_HEADER_SIZE;
}

TableStore::~TableStore() {
  for (auto tuple_group : tuple_groups_) free(tuple_group);
}

bool TableStore::insertTuple(std::vector<Expr*>* values) {
  if (free_list_.isEmpty())
    if (newTupleGroup()) return true;

  Tuple* tup = free_list_.popHead();
  data_list_.addHead(tup);

  int idx = 0;
  for (auto expr : *values) {
    setColValue(tup, idx, expr);
    idx++;
  }

  if (g_transaction.inTransaction()) g_transaction.addInsertUndo(this, tup);

  return false;
}

bool TableStore::deleteTuple(Tuple* tup) {
  data_list_.delTuple(tup);
  free_list_.addHead(tup);
  if (g_transaction.inTransaction()) g_transaction.addDeleteUndo(this, tup);

  return true;
}

void TableStore::removeTuple(Tuple* tup) {
  data_list_.delTuple(tup);
  free_list_.addHead(tup);
}

void TableStore::recoverTuple(Tuple* tup) { data_list_.addHead(tup); }

void TableStore::freeTuple(Tuple* tup) { free_list_.addHead(tup); }

bool TableStore::updateTuple(Tuple* tup, std::vector<size_t>& idxs,
                             std::vector<Expr*>& values) {
  if (g_transaction.inTransaction()) g_transaction.addUpdateUndo(this, tup);

  for (size_t i = 0; i < idxs.size(); i++) {
    size_t idx = idxs[i];
    Expr* expr = values[i];
    setColValue(tup, idx, expr);
  }

  return false;
}

Tuple* TableStore::seqScan(Tuple* tup) {
  if (tup == nullptr)
    return data_list_.getHead();
  else
    return data_list_.getNext(tup);
}

void TableStore::parseTuple(Tuple* tup, std::vector<Expr*>& values) {
  bool* is_null = reinterpret_cast<bool*>(&tup->data[0]);
  uchar* data = tup->data + columns_->size();

  for (size_t i = 0; i < columns_->size(); i++) {
    Expr* e = nullptr;
    if (is_null[i]) {
      e = Expr::makeNullLiteral();
      values.emplace_back(e);
      continue;
    }

    ColumnDefinition* col = (*columns_)[i];
    int offset = col_offset_[i];
    int size = col_offset_[i + 1] - col_offset_[i];
    switch (col->type.data_type) {
      case DataType::INT: {
        int64_t val = *reinterpret_cast<int32_t*>(data + offset);
        e = Expr::makeLiteral(val);
        break;
      }
      case DataType::LONG: {
        int64_t val = *reinterpret_cast<int64_t*>(data + offset);
        e = Expr::makeLiteral(val);
        break;
      }
      case DataType::DOUBLE: {
        double val = *reinterpret_cast<double*>(data + offset);
        e = Expr::makeLiteral(val);
        break;
      }
      case DataType::CHAR:
      case DataType::VARCHAR: {
        char* val = static_cast<char*>(malloc(size));
        memcpy(val, (data + offset), size);
        e = Expr::makeLiteral(val);
        break;
      }
      default:
        break;
    }
    values.emplace_back(e);
  }
}

bool TableStore::newTupleGroup() {
  Tuple* tuple_group =
      static_cast<Tuple*>(malloc(tuple_size_ * TUPLE_GROUP_SIZE));
  memset(tuple_group, 0, (tuple_size_ * TUPLE_GROUP_SIZE));
  if (tuple_group == nullptr) {
    std::cout << "[LiteDB-Error]  Failed to malloc "
              << tuple_size_ * TUPLE_GROUP_SIZE << " bytes";
    return true;
  }

  tuple_groups_.emplace_back(tuple_group);
  uchar* ptr = reinterpret_cast<uchar*>(tuple_group);
  for (int i = 0; i < TUPLE_GROUP_SIZE; i++) {
    Tuple* tup = reinterpret_cast<Tuple*>(ptr);
    free_list_.addHead(tup);
    ptr += tuple_size_;
  }

  return false;
}

void TableStore::setColValue(Tuple* tup, int idx, Expr* expr) {
  bool* is_null = reinterpret_cast<bool*>(&tup->data[0]);
  uchar* data = tup->data + col_num_;
  int offset = col_offset_[idx];
  int size = col_offset_[idx + 1] - col_offset_[idx];
  uchar* ptr = &data[offset];
  is_null[idx] = false;

  switch (expr->type) {
    case kExprLiteralInt: {
      if (size == 4)
        *reinterpret_cast<int32_t*>(ptr) = static_cast<int>(expr->ival);
      else
        *reinterpret_cast<int64_t*>(ptr) = expr->ival;
      break;
    }
    case kExprLiteralFloat: {
      if (size == 4)
        *reinterpret_cast<float*>(ptr) = static_cast<float>(expr->fval);
      else
        *reinterpret_cast<double*>(ptr) = expr->fval;
      break;
    }
    case kExprLiteralString: {
      int len = strlen(expr->name);
      memcpy(ptr, expr->name, len);
      ptr[len] = '\0';
      break;
    }
    case kExprLiteralNull: {
      is_null[idx] = true;
      break;
    }
    default:
      break;
  }
}

}  // namespace litedb