#include "metadata.h"

#include <iostream>

#include "util.h"

using namespace hsql;

namespace litedb {

MetaData g_meta_data;

Table::Table(char* schema, char* name,
             std::vector<ColumnDefinition*>* columns) {
  schema_ = strdup(schema);
  name_ = strdup(name);
  for (auto col_old : *columns) {
    std::unordered_set<ConstraintType>* column_constraints =
        new std::unordered_set<ConstraintType>();
    *column_constraints = *col_old->column_constraints;
    ColumnDefinition* col = new ColumnDefinition(
        strdup(col_old->name), col_old->type, column_constraints);
    col->nullable = col_old->nullable;
    columns_.emplace_back(col);
  }

  table_store_ = new TableStore(&columns_);
}

Table::~Table() {
  free(schema_);
  free(name_);
  delete table_store_;
  for (auto col : columns_) delete col;
}

ColumnDefinition* Table::getColumn(char* name) {
  if (name == nullptr || strlen(name) == 0) return nullptr;

  for (auto col : columns_)
    if (strcmp(name, col->name) == 0) return col;

  return nullptr;
}

Index* Table::getIndex(char* name) {
  if (name == nullptr || strlen(name) == 0) return nullptr;

  for (auto index : indexes_)
    if (strcmp(name, index->name)) return index;

  return nullptr;
}

bool MetaData::insertTable(Table* table) {
  if (getTable(table->schema(), table->name()) != nullptr) return true;

  TableName table_name;
  SetTableName(table_name, table->schema(), table->name());
  table_map_.emplace(table_name, table);
  return false;
}

bool MetaData::dropIndex(char* schema, char* name, char* indexName) {
  Table* table = getTable(schema, name);
  if (table == nullptr) {
    std::cout << "[LiteDB-Error]  Table " << TableNameToString(schema, name)
              << " did not exist!\r\n";
    return true;
  }

  std::vector<Index*>& indexes = *table->indexes();
  for (size_t i = 0; i < indexes.size(); i++) {
    Index* index = indexes[i];
    if (strcmp(index->name, indexName) == 0) {
      indexes.erase(indexes.begin() + i);
      return false;
    }
  }

  return true;
}

bool MetaData::dropIndex(char* schema, char* name, char* indexName,
                         Index** index) {
  if (index == nullptr) return true;

  Table* table = getTable(schema, name);
  if (table == nullptr) {
    std::cout << "[LiteDB-Error]  Table " << TableNameToString(schema, name)
              << " did not exist!\r\n";
    return true;
  }

  std::vector<Index*>& indexes = *table->indexes();
  for (size_t i = 0; i < indexes.size(); i++) {
    *index = indexes[i];
    if (strcmp((*index)->name, indexName) == 0) return false;
  }

  return true;
}

bool MetaData::dropTable(char* schema, char* name) {
  Table* table = getTable(schema, name);
  if (table == nullptr) return true;

  TableName table_name;
  SetTableName(table_name, schema, name);
  table_map_.erase(table_name);
  delete table;

  return false;
}

// 第三个参数返回 Table 的地址
bool MetaData::dropTable(char* schema, char* name, Table** table) {
  if (table == nullptr) return true;

  *table = getTable(schema, name);
  if (table == nullptr) return true;

  TableName table_name;
  SetTableName(table_name, schema, name);
  table_map_.erase(table_name);
  return false;
}

bool MetaData::dropSchema(char* schema) {
  auto iter = table_map_.begin();
  bool ret = true;
  while (iter != table_map_.end()) {
    Table* table = iter->second;
    if (strcmp(table->schema(), schema) == 0) {
      std::cout << "[LiteDB-Info]  Drop table " << table->name()
                << " in schema " << schema << "\r\n";
      iter = table_map_.erase(iter);
      delete table;
      ret = false;
    } else {
      iter++;
    }
  }
  return ret;
}

// 第二个参数返回 Tables 的地址
bool MetaData::dropSchema(char* schema, std::vector<Table*>* tables) {
  auto iter = table_map_.begin();
  bool ret = true;
  while (iter != table_map_.end()) {
    Table* table = iter->second;
    if (strcmp(table->schema(), schema) == 0) {
      std::cout << "[LiteDB-Info]  Drop table " << table->name()
                << " in schema " << schema << "\r\n";
      iter = table_map_.erase(iter);
      tables->emplace_back(table);
      ret = false;
    } else {
      iter++;
    }
  }
  return ret;
}

void MetaData::getAllTables(std::vector<Table*>* tables) {
  if (tables == nullptr) return;

  for (auto iter : table_map_) tables->emplace_back(iter.second);
}

bool MetaData::findSchema(char* schema) {
  for (auto iter : table_map_) {
    Table* table = iter.second;
    if (strcmp(table->schema(), schema) == 0) return true;
  }

  return false;
}

Table* MetaData::getTable(char* schema, char* name) {
  if (schema == nullptr || name == nullptr) {
    std::cout << "[LiteDB-Error]: Schema and table name should be specified in "
                 "the query, like 'db.t'.\r\n";
    return nullptr;
  }
  TableName table_name;
  SetTableName(table_name, schema, name);

  auto iter = table_map_.find(table_name);
  if (iter == table_map_.end())
    return nullptr;
  else
    return iter->second;
}

Index* MetaData::getIndex(char* schema, char* name, char* index_name) {
  Table* table = getTable(schema, name);
  if (table == nullptr) {
    std::cout << "[LiteDB-Error]  Table " << TableNameToString(schema, name)
              << " did not exist!\\r\n";
    return nullptr;
  }

  for (auto index : *table->indexes())
    if (strcmp(index->name, index_name) == 0) return index;

  return nullptr;
}

}  // namespace litedb