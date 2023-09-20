set(Lite_DB_SRC
  main.cpp
  executor/executor.cpp
  storage/metadata.cpp
  executor/optimizer.cpp
  parser/parser.cpp
  storage/storage.cpp
  trx.cpp
  util.cpp
)

add_executable(LiteDB
  ${Lite_DB_SRC})

target_link_libraries(LiteDB
  ${CMAKE_SOURCE_DIR}/lib/libsqlparser.so)