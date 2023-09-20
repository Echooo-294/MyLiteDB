#pragma once

#include <string>

#include "executor/optimizer.h"
#include "sql/ColumnType.h"
#include "sql/Table.h"
#include "sql/statements.h"
#include "storage/storage.h"

using namespace hsql;

namespace litedb {
inline bool IsDataTypeSupport(DataType type) {
  return (type == DataType::INT || type == DataType::LONG ||
          type == DataType::CHAR || type == DataType::VARCHAR ||
          type == DataType::DOUBLE);
}

inline std::string TableNameToString(char* schema, char* name) {
  std::string str =
      (schema == nullptr) ? name : schema + std::string(".") + name;
  return str;
}

inline void SetTableName(TableName& table_name, char* schema, char* name) {
  table_name.schema = schema;
  table_name.name = name;
}

const char* StmtTypeToString(StatementType type);
const char* DataTypeToString(DataType type);
const char* DropTypeToString(DropType type);
const char* ExprTypeToString(ExprType type);
const char* PlanTypeToString(PlanType type);

size_t ColumnTypeSize(ColumnType& type);

void PrintTuples(std::vector<ColumnDefinition*>& columns,
                 std::vector<size_t>& colIds,
                 std::vector<std::vector<Expr*>>& tuples);

}  // namespace litedb