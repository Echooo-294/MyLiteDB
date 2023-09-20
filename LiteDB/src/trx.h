#pragma once

#include <stack>
#include <vector>

#include "storage/metadata.h"
#include "storage/storage.h"
#include "util.h"

namespace litedb {
enum UndoType {
  kInsertUndo,
  kUpdateUndo,
  kDeleteUndo,
  kCreateTableUndo,
  kCreateIndexUndo,
  kDropSchemaUndo,
  kDropTableUndo,
  kDropIndexUndo,
};

struct Undo {
  Undo(UndoType t)
      : type(t), tableStore(nullptr), curTup(nullptr), oldTup(nullptr) {}
  ~Undo() {
    // 根据 Undo 的类型进行一些内存释放
    switch (type) {
      case kUpdateUndo:
        free(oldTup);
        break;
      case kCreateTableUndo:
        delete schema;
        delete name;
        break;
      case kCreateIndexUndo:
        delete schema;
        delete name;
        delete index_name;
        break;
      case kDropIndexUndo:
        delete schema;
        delete name;
        break;
      default:
        break;
    }
  }

  UndoType type;
  TableStore* tableStore;
  Tuple* curTup;
  Tuple* oldTup;
  char* schema;
  char* name;
  char* index_name;
  std::vector<Table*> tables;
  Index* index;
};

class Transaction {
 public:
  Transaction() : in_transaction_(false) {}
  ~Transaction() {}

  void addInsertUndo(TableStore* table_store, Tuple* tup);
  void addDeleteUndo(TableStore* table_store, Tuple* tup);
  void addUpdateUndo(TableStore* table_store, Tuple* tup);
  void addCreateTableUndo(char* schema, char* name);
  void addCreateIndexUndo(char* schema, char* name, char* index_name);
  void addDropSchemaUndo(std::vector<Table*>* table);
  void addDropTableUndo(Table* table);
  void addDropIndexUndo(char* schema, char* table_name, Index* index);

  void begin();
  void rollback();
  void commit();

  bool inTransaction() { return in_transaction_; }

 private:
  bool in_transaction_;
  std::stack<Undo*> undo_stack_;
};

extern Transaction g_transaction;
}  // namespace litedb