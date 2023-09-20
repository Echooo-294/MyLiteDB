#include <stdlib.h>
#include <termios.h>

#include <iostream>
#include <string>

#include "executor/executor.h"
#include "executor/optimizer.h"
#include "parser/parser.h"

using namespace litedb;
using namespace hsql;

static uint8_t cmdline_cnt = 0;

struct CMDLine {
  CMDLine *pre, *nxt;
  std::string cmd;
};

static bool ExecStmt(std::string stmt);

static void HandleInput();

int main(int argc, char* argv[]) {
  struct termios tm, tm_old;
  int fd = 0;
  // 保存当前终端状态
  if (tcgetattr(fd, &tm) < 0) return -1;
  tm_old = tm;
  cfmakeraw(&tm);  // 切换到原始模式
  if (tcsetattr(fd, TCSANOW, &tm) < 0) return -1;

  std::cout << "# Welcome to Lite DB!\r\n";
  std::cout << "# Input sql in one line.\r\n";
  std::cout << "# Enter 'exit' or 'q' to quit this program.\r\n";

  // 内部循环处理
  HandleInput();

  std::cout << "# Bye~\r\n";

  // 恢复终端状态
  if (tcsetattr(fd, TCSANOW, &tm_old) < 0) return -1;

  return 0;
}

static bool ExecStmt(std::string stmt) {
  Parser parser;
  if (parser.parseStatement(stmt)) return true;

  SQLParserResult* result = parser.getResult();
  Optimizer optimizer;

  for (size_t i = 0; i < result->size(); ++i) {
    // 通过 SQL Parser 获取输入相关信息
    const SQLStatement* stmt = result->getStatement(i);
    Plan* plan = optimizer.createPlanTree(stmt);
    if (plan == nullptr) return true;

    Executor executor(plan);
    executor.init();
    if (executor.exec()) return true;
  }

  return false;
}

static void HandleInput() {
  CMDLine *head, *tail, *cur;
  head = new (CMDLine);
  tail = new (CMDLine);
  cur = new (CMDLine);
  head->nxt = cur;
  tail->pre = cur;
  cur->pre = head;
  cur->nxt = tail;

  while (true) {
    std::cout << ">> ";

    std::size_t idx = 0;
    char c;
    CMDLine* p = cur;

    while (true) {
      // 根据字符判断处理逻辑
      if (c == '\r') {
        c = 0;
        break;
      }
      c = getchar();

      size_t erase_len;
      switch (c) {
        case -1:  // no input
          continue;

        case 27:  // ↑ ↓ ← →
          getchar();
          c = getchar();
          switch (c) {
            case 65:  // ↑
              if (head->nxt == p) continue;
              for (std::size_t i = 0; i < idx; i++) std::cout << '\b';
              for (std::size_t i = 0; i < cur->cmd.length(); i++)
                std::cout << ' ';
              for (std::size_t i = 0; i < cur->cmd.length(); i++)
                std::cout << '\b';
              p = p->pre;
              cur->cmd = p->cmd;
              std::cout << cur->cmd;
              idx = cur->cmd.length();
              continue;
            case 66:  // ↓
              if (tail->pre == p) continue;
              for (std::size_t i = 0; i < idx; i++) std::cout << '\b';
              for (std::size_t i = 0; i < cur->cmd.length(); i++)
                std::cout << ' ';
              for (std::size_t i = 0; i < cur->cmd.length(); i++)
                std::cout << '\b';
              p = p->nxt;
              cur->cmd = p->cmd;
              std::cout << cur->cmd;
              idx = cur->cmd.length();
              continue;
            case 67:  // →
              if (idx < cur->cmd.length())
                idx++;
              else
                continue;
              break;
            case 68:  // ←
              if (idx > 0)
                idx--;
              else
                continue;
              break;
            default:
              break;
          }
          std::cout << char(27) << char(91);
          std::cout << c;
          continue;

        case '\r':  // EOF
          std::cout << "\r\n";
          continue;

        case '\b':  // 空格
          if (idx == 0) continue;
          erase_len = cur->cmd.length() - idx + 1;
          cur->cmd.erase(idx - 1, 1);
          std::cout << '\b';
          for (std::size_t i = 0; i < erase_len; i++) std::cout << " ";
          for (std::size_t i = 0; i < erase_len; i++) std::cout << '\b';
          idx--;
          for (std::size_t i = 0; i < erase_len - 1; i++)
            std::cout << cur->cmd[idx + i];
          for (std::size_t i = 0; i < erase_len - 1; i++) std::cout << '\b';

          break;

        default:
          erase_len = cur->cmd.length() - idx;
          cur->cmd.insert(idx, 1, c);
          for (std::size_t i = 0; i < erase_len; i++) std::cout << " ";
          for (std::size_t i = 0; i < erase_len; i++) std::cout << '\b';
          for (std::size_t i = 0; i < erase_len + 1; i++)
            std::cout << cur->cmd[idx + i];
          for (std::size_t i = 0; i < erase_len; i++) std::cout << '\b';
          idx++;
      }
    }
    if (cur->cmd.length() == 0) continue;

    if (cur->cmd == "exit" || cur->cmd == "q") break;

    if (ExecStmt(cur->cmd))
      std::cout << "[LiteDB-Error]  Failed to execute '" << cur->cmd << "'"
                << "\r\n";

    tail->nxt = new (CMDLine);
    cur = tail;
    tail = tail->nxt;
    tail->pre = cur;

    // 64 是缓存命令行的最大值.
    if (cmdline_cnt++ == 64) {
      CMDLine* t = head;
      head = head->nxt;
      delete t;
    }

    std::cout << "\r\n";
  }
}