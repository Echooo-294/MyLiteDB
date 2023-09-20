#include "trx.h"

namespace litedb {
Transaction g_transaction;

void Transaction::addInsertUndo(TableStore* table_store, Tuple* tup) {
  Undo* undo = new Undo(kInsertUndo);
  undo->tableStore = table_store;
  undo->curTup = tup;
  undo_stack_.emplace(undo);
}

void Transaction::addUpdateUndo(TableStore* table_store, Tuple* tup) {
  Undo* undo = new Undo(kUpdateUndo);
  undo->tableStore = table_store;
  undo->oldTup = static_cast<Tuple*>(malloc(table_store->tupleSize()));
  memcpy(undo->oldTup->data, tup->data,
         table_store->tupleSize() - TUPLE_HEADER_SIZE);
  undo->curTup = tup;
  undo_stack_.emplace(undo);
}

void Transaction::addDeleteUndo(TableStore* table_store, Tuple* tup) {
  Undo* undo = new Undo(kDeleteUndo);
  undo->tableStore = table_store;
  undo->oldTup = tup;
  undo_stack_.emplace(undo);
}

void Transaction::addCreateTableUndo(char* schema, char* name) {
  Undo* undo = new Undo(kCreateTableUndo);
  undo->schema = schema;
  undo->name = name;
  undo_stack_.emplace(undo);
}

void Transaction::addCreateIndexUndo(char* schema, char* name,
                                     char* index_name) {
  Undo* undo = new Undo(kCreateIndexUndo);
  undo->schema = schema;
  undo->name = name;
  undo->index_name = index_name;
  undo_stack_.emplace(undo);
}

void Transaction::addDropSchemaUndo(std::vector<Table*>* tables) {
  Undo* undo = new Undo(kDropSchemaUndo);
  for (const auto table : *tables) undo->tables.emplace_back(table);
  undo_stack_.emplace(undo);
}

void Transaction::addDropTableUndo(Table* table) {
  Undo* undo = new Undo(kDropTableUndo);
  undo->tables.emplace_back(table);
  undo_stack_.emplace(undo);
}

void Transaction::addDropIndexUndo(char* schema, char* name, Index* index) {
  Undo* undo = new Undo(kDropIndexUndo);
  undo->schema = schema;
  undo->name = name;
  undo->index = index;
  undo_stack_.emplace(undo);
}

void Transaction::begin() { in_transaction_ = true; }

void Transaction::rollback() {
  while (!undo_stack_.empty()) {
    auto undo = undo_stack_.top();
    TableStore* table_store = undo->tableStore;
    undo_stack_.pop();
    // 根据 Undo 的类型决定对储存下来的数据有何改变。
    switch (undo->type) {
      case kInsertUndo:
        table_store->removeTuple(undo->curTup);
        break;
      case kDeleteUndo:
        table_store->recoverTuple(undo->oldTup);
        break;
      case kUpdateUndo:
        memcpy(undo->curTup->data, undo->oldTup->data,
               table_store->tupleSize() - TUPLE_HEADER_SIZE);
        break;
      case kCreateTableUndo:
        g_meta_data.dropTable(undo->schema, undo->name);
        break;
      case kCreateIndexUndo:
        g_meta_data.dropIndex(undo->schema, undo->name, undo->index_name);
        break;
      case kDropSchemaUndo:
      case kDropTableUndo:
        for (const auto table : undo->tables) g_meta_data.insertTable(table);
        break;
      case kDropIndexUndo: {
        Table* table = g_meta_data.getTable(undo->schema, undo->name);
        table->addIndex(undo->index);
        break;
      }
      default:
        break;
    }
    delete undo;
  }
  in_transaction_ = false;
}

void Transaction::commit() {
  while (!undo_stack_.empty()) {
    auto undo = undo_stack_.top();
    TableStore* table_store = undo->tableStore;
    undo_stack_.pop();
    // 根据 Undo 的类型决定哪些内存需要释放。
    switch (undo->type) {
      case kDeleteUndo:
        table_store->freeTuple(undo->oldTup);
        break;
      case kDropSchemaUndo:
      case kDropTableUndo:
        for (const auto table : undo->tables) delete table;
        break;
      case kDropIndexUndo:
        delete undo->index;
        break;
      default:
        break;
    }
    delete undo;
  }
  in_transaction_ = false;
}

}  // namespace litedb