#pragma once

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <unordered_map>
#include <vector>

#include "sql/statements.h"

using namespace hsql;

namespace litedb {

#define TUPLE_GROUP_SIZE 100
#define TUPLE_HEADER_SIZE 16

typedef unsigned char uchar;

struct Tuple {
  Tuple* prev;
  Tuple* next;
  uchar data[];
};

class TupleList {
 public:
  TupleList() {
    head_ = static_cast<Tuple*>(malloc(sizeof(Tuple)));
    tail_ = static_cast<Tuple*>(malloc(sizeof(Tuple)));
    head_->next = tail_;
    tail_->prev = head_;
    head_->prev = nullptr;
    tail_->next = nullptr;
  }

  void addHead(Tuple* tup) {
    Tuple* ntup = head_->next;
    ntup->prev = tup;
    tup->next = ntup;
    head_->next = tup;
    tup->prev = head_;
  }

  void delTuple(Tuple* tup) {
    Tuple* ntup = tup->next;
    Tuple* ptup = tup->prev;
    ptup->next = ntup;
    ntup->prev = ptup;
    tup->next = nullptr;
    tup->prev = nullptr;
  }

  Tuple* popHead() {
    if (head_->next == tail_) {
      return nullptr;
    }

    Tuple* tup = head_->next;
    delTuple(tup);
    return tup;
  }

  Tuple* getHead() {
    if (head_->next == tail_) {
      return nullptr;
    }
    return head_->next;
  }

  Tuple* getNext(Tuple* tup) {
    return (tup->next == tail_) ? nullptr : tup->next;
  }

  bool isEmpty() { return (head_->next == tail_); }

 private:
  Tuple* head_;
  Tuple* tail_;
};

class TableStore {
 public:
  TableStore(std::vector<ColumnDefinition*>* columns);
  ~TableStore();

  bool insertTuple(std::vector<Expr*>* values);
  bool deleteTuple(Tuple* tup);
  bool updateTuple(Tuple* tup, std::vector<size_t>& idxs,
                   std::vector<Expr*>& values);

  void removeTuple(Tuple* tup);
  void recoverTuple(Tuple* tup);
  void freeTuple(Tuple* tup);

  Tuple* seqScan(Tuple* tup);
  void parseTuple(Tuple* tup, std::vector<Expr*>& values);

  int tupleSize() { return tuple_size_; }

 private:
  bool newTupleGroup();
  void setColValue(Tuple* tup, int idx, Expr* expr);

  int col_num_;
  int tuple_size_;

  std::vector<ColumnDefinition*>* columns_;
  std::vector<int> col_offset_;
  std::vector<Tuple*> tuple_groups_;
  TupleList free_list_;
  TupleList data_list_;
};

}  // namespace litedb