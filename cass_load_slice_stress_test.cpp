#include <gflags/gflags.h>

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstring>
#include <functional>
#include <iostream>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cass/include/cassandra.h"

DEFINE_string(cass_endpoints, "127.0.0.1",
              "Cassandra endpoints, comma separated");
DEFINE_int32(cass_port, 9042, "Cassandra port");
DEFINE_string(cass_keyspace, "mono", "Cassandra keyspace");
DEFINE_string(cass_username, "cassandra", "Cassandra username");
DEFINE_string(cass_password, "cassandra", "Cassandra password");
DEFINE_uint64(cass_queue_size_io, 4096, "Cassandra client queue size io");
DEFINE_uint32(cass_num_threads_io, 3, "Cassandra number of threads io");
DEFINE_uint32(cass_core_connections_per_host, 1,
              "Cassandra connections per host");

DEFINE_string(mono_database_name, "sbtest", "Monograph database");
DEFINE_string(mono_table_names, "sbtest1, sbtest2, sbtest3",
              "Monograph tables, comma separated");
DEFINE_string(mono_table_columns, "c, t", "Monograph columns, comma separated");
DEFINE_string(mono_table_column_types, "blob, int",
              "Monograph column types, comma separated");

DEFINE_int32(num_threads, 10, "Number of threads");
DEFINE_int32(max_flying_req_num, 100, "Max flying request number");
DEFINE_int32(test_duration, 60, "Test duration in seconds");
DEFINE_int32(report_interval, 5, "Report interval in seconds");

typedef unsigned char uchar; /* Short for unsigned char */

static const char MARIADB_TABLES[] = "mariadb_tables";
static const char TABLE_RANGES[] = "table_ranges";

bool debug_output = false;

class TableSlice {
 public:
  TableSlice() = delete;
  TableSlice(int32_t partition_id, const uchar *start_key,
             const size_t start_key_size, const uchar *end_key,
             const size_t end_key_size)
      : partition_id_(partition_id) {
    start_key_.clear();
    start_key_.resize(start_key_size);
    memcpy(start_key_.data(), start_key, start_key_size);
    end_key_.clear();
    end_key_.resize(end_key_size);
    memcpy(end_key_.data(), end_key, end_key_size);
  }

  const std::vector<char> &start_key() const { return start_key_; }
  const std::vector<char> &end_key() const { return end_key_; }
  const int32_t partition_id() const { return partition_id_; }

 private:
  std::vector<char> start_key_;
  std::vector<char> end_key_;
  int32_t partition_id_;
};

auto now() { return std::chrono::high_resolution_clock::now(); }

inline std::string trim(std::string &str) {
  str.erase(str.find_last_not_of(' ') + 1);  // suffixing spaces
  str.erase(0, str.find_first_not_of(' '));  // prefixing spaces
  return str;
}

std::vector<std::string> SplitString(const std::string &input, char delimiter) {
  std::vector<std::string> result;
  std::stringstream ss(input);
  std::string item;

  while (std::getline(ss, item, delimiter)) {
    result.push_back(trim(item));
  }

  return result;
}

std::string_view ErrorMessage(CassFuture *future) {
  const char *message;
  size_t length;
  cass_future_error_message(future, &message, &length);
  return {message, length};
}

static const std::string GetUniformTableName(const std::string &database_name,
                                             const std::string &table_name) {
  return "./" + database_name + "/" + table_name;
}

class TableConfig {
 public:
  TableConfig(const std::string &database_name, const std::string &table_name,
              const std::string &keyspace, const std::string &kv_table_name,
              const std::vector<std::string> &columns,
              const std::vector<std::string> &column_types,
              const std::vector<TableSlice> &table_slices)
      : database_name_(database_name),
        table_name_(table_name),
        keyspace_(keyspace),
        kv_table_name_(kv_table_name),
        columns_(std::move(columns)),
        column_types_(std::move(column_types)),
        table_slices_(std::move(table_slices)) {}

  const std::string &database_name() const { return database_name_; }
  const std::string &table_name() const { return table_name_; }
  const std::string &keyspace() const { return keyspace_; }
  const std::string &kv_table_name() const { return kv_table_name_; }
  const std::vector<std::string> &columns() const { return columns_; }
  const std::vector<std::string> &column_types() const { return column_types_; }
  const std::vector<TableSlice> &table_slices() const { return table_slices_; }
  const std::string GetUniformTableName() {
    return ::GetUniformTableName(database_name_, table_name_);
  }
  void print_debug_info() {
    std::cout << "table_name: " << table_name_ << std::endl;
    std::cout << "keyspace: " << keyspace_ << std::endl;
    std::cout << "kv_table_name: " << kv_table_name_ << std::endl;
    std::cout << "columns: ";
    for (auto &column : columns_) {
      std::cout << column << ", ";
    }
    std::cout << std::endl;
    std::cout << "column_types: ";
    for (auto &column_type : column_types_) {
      std::cout << column_type << ", ";
    }
    std::cout << std::endl;
    std::cout << "table_slices: " << table_slices_.size() << std::endl;
  }

 private:
  std::string keyspace_;
  std::string database_name_;
  std::string table_name_;
  std::string kv_table_name_;
  std::vector<std::string> columns_;
  std::vector<std::string> column_types_;
  std::vector<TableSlice> table_slices_;
};

class CassHandler {
 public:
  CassHandler(const std::string &cass_endpoints, const uint32_t cass_port,
              const std::string &cass_username,
              const std::string &cass_password,
              const uint64_t cass_queue_size_io,
              const uint32_t cass_num_threads_io,
              const uint32_t cass_core_connections_per_host)
      : cluster_(nullptr), session_(nullptr) {
    cluster_ = cass_cluster_new();
    cass_cluster_set_contact_points(cluster_, cass_endpoints.c_str());
    cass_cluster_set_port(cluster_, cass_port);
    cass_cluster_set_credentials(cluster_, cass_username.c_str(),
                                 cass_password.c_str());
    cass_cluster_set_queue_size_io(cluster_, cass_queue_size_io);
    cass_cluster_set_num_threads_io(cluster_, cass_num_threads_io);
    cass_cluster_set_core_connections_per_host(cluster_,
                                               cass_core_connections_per_host);
    session_ = cass_session_new();
  }

  ~CassHandler() {
    cass_session_free(session_);
    cass_cluster_free(cluster_);
  }

  bool Connect() {
    CassFuture *connect_future = cass_session_connect(session_, cluster_);
    cass_future_wait(connect_future);
    CassError rc = cass_future_error_code(connect_future);
    if (rc != CASS_OK) {
      std::cerr << "Failed to connect to cluster: " << rc << std::endl;
      cass_future_free(connect_future);
      return false;
    }
    cass_future_free(connect_future);
    return true;
  }

  bool LoadTableConfig(const std::string &keyspace,
                       const std::string &database_name,
                       const std::string &table_name,
                       const std::vector<std::string> &table_columns,
                       const std::vector<std::string> &table_column_types) {
    std::lock_guard<std::mutex> lock(mutex_);
    // load mono table config
    std::string tablename = GetUniformTableName(database_name, table_name);
    std::string table_config_query =
        "SELECT tablename, kvtablename, kvindexname FROM " + keyspace + "." +
        MARIADB_TABLES + " WHERE tablename = ?";

    if (debug_output) {
      std::cout << "table_config_query: " << table_config_query
                << " ,tablename: " << tablename << std::endl;
    }
    CassStatement *table_config_statement =
        cass_statement_new(table_config_query.c_str(), 1);
    cass_statement_bind_string(table_config_statement, 0, tablename.c_str());
    CassFuture *table_config_result_future =
        cass_session_execute(session_, table_config_statement);
    cass_future_wait(table_config_result_future);
    CassError rc = cass_future_error_code(table_config_result_future);
    if (rc != CASS_OK) {
      std::cerr << "Failed to execute query: "
                << ErrorMessage(table_config_result_future) << std::endl;
      cass_future_free(table_config_result_future);
      cass_statement_free(table_config_statement);
      return false;
    }

    const CassResult *table_config_result =
        cass_future_get_result(table_config_result_future);
    CassIterator *table_config_iterator =
        cass_iterator_from_result(table_config_result);
    std::string kvtablename;
    std::string kvindexname;
    cass_bool_t has_next = cass_iterator_next(table_config_iterator);
    if (has_next == cass_bool_t::cass_false) {
      std::cerr << "Failed to get table config" << std::endl;
      cass_future_free(table_config_result_future);
      cass_statement_free(table_config_statement);
      return false;
    }
    const CassRow *table_config_row =
        cass_iterator_get_row(table_config_iterator);
    const CassValue *table_config_value =
        cass_row_get_column_by_name(table_config_row, "kvtablename");
    const char *kvtablename_data;
    size_t kvtablename_length;
    cass_value_get_string(table_config_value, &kvtablename_data,
                          &kvtablename_length);
    kvtablename = std::string(kvtablename_data, kvtablename_length);
    table_config_value =
        cass_row_get_column_by_name(table_config_row, "kvindexname");
    const char *kvindexname_data;
    size_t kvindexname_length;
    cass_value_get_string(table_config_value, &kvindexname_data,
                          &kvindexname_length);
    kvindexname = std::string(kvindexname_data, kvindexname_length);
    cass_future_free(table_config_result_future);
    cass_statement_free(table_config_statement);

    // load table slices
    std::string table_range_query =
        "SELECT tablename, \"___mono_key___\", \"___partition_id___\", "
        "\"___version___\", \"___slice_keys___\", \"___slice_sizes___\" FROM " +
        keyspace + "." + TABLE_RANGES +
        " WHERE tablename = ? ORDER BY \"___mono_key___\" ASC";
    if (debug_output) {
      std::cout << "table_range_query: " << table_range_query
                << " ,tablename: " << tablename << std::endl;
    }
    CassStatement *table_range_statement =
        cass_statement_new(table_range_query.c_str(), 1);
    cass_statement_bind_string(table_range_statement, 0, tablename.c_str());
    CassFuture *table_range_result_future =
        cass_session_execute(session_, table_range_statement);
    cass_future_wait(table_range_result_future);
    rc = cass_future_error_code(table_range_result_future);
    if (rc != CASS_OK) {
      std::cerr << "Failed to execute query: "
                << ErrorMessage(table_config_result_future) << std::endl;
      cass_future_free(table_config_result_future);
      cass_statement_free(table_config_statement);
      return false;
    }

    std::vector<TableSlice> table_slices;
    const CassResult *table_range_result =
        cass_future_get_result(table_range_result_future);
    CassIterator *table_range_iterator =
        cass_iterator_from_result(table_range_result);
    while (cass_iterator_next(table_range_iterator)) {
      const CassRow *table_range_row =
          cass_iterator_get_row(table_range_iterator);
      // get start key
      const CassValue *mono_key_value =
          cass_row_get_column_by_name(table_range_row, "___mono_key___");
      const cass_byte_t *mono_key_data;
      size_t mono_key_size;
      cass_value_get_bytes(mono_key_value, &mono_key_data, &mono_key_size);
      // get partition id
      const CassValue *partition_id_value =
          cass_row_get_column_by_name(table_range_row, "___partition_id___");
      int32_t partition_id;
      cass_value_get_int32(partition_id_value, &partition_id);

      // get version
      const CassValue *version_value =
          cass_row_get_column_by_name(table_range_row, "___version___");
      int64_t version;
      cass_value_get_int64(version_value, &version);

      // get slice keys
      int slice_cnt = 0;
      const cass_byte_t *slice_start_key_data = mono_key_data;
      size_t slice_start_key_size = mono_key_size;
      const CassValue *slice_keys_value =
          cass_row_get_column_by_name(table_range_row, "___slice_keys___");
      CassIterator *slice_keys_iterator =
          cass_iterator_from_collection(slice_keys_value);
      while (cass_iterator_next(slice_keys_iterator)) {
        const CassValue *slice_key_value =
            cass_iterator_get_value(slice_keys_iterator);
        const cass_byte_t *slice_key_data;
        size_t slice_key_size;
        cass_value_get_bytes(slice_key_value, &slice_key_data, &slice_key_size);
        table_slices.emplace_back(partition_id, slice_start_key_data,
                                  slice_start_key_size, slice_key_data,
                                  slice_key_size);
        slice_start_key_data = slice_key_data;
        slice_start_key_size = slice_key_size;
      }
    }
    cass_future_free(table_range_result_future);
    cass_statement_free(table_range_statement);
    TableConfig table_config(database_name, table_name, keyspace, kvtablename,
                             table_columns, table_column_types, table_slices);
    table_config.print_debug_info();
    table_configs_.emplace(std::make_pair(table_config.GetUniformTableName(),
                                          std::move(table_config)));

    return true;
  }

  static void OnLoadSlices(CassFuture *load_slice_result_future, void *data) {
    auto *callback = static_cast<std::function<void(size_t)> *>(data);
    CassError rc = cass_future_error_code(load_slice_result_future);
    if (rc != CASS_OK) {
      std::cerr << "Failed to execute query: "
                << ErrorMessage(load_slice_result_future) << std::endl;
      cass_future_free(load_slice_result_future);
      (*callback)(0);
      return;
    }
    const CassResult *load_slice_result =
        cass_future_get_result(load_slice_result_future);
    CassIterator *load_slice_iterator =
        cass_iterator_from_result(load_slice_result);
    size_t row_cnt = 0;
    while (cass_iterator_next(load_slice_iterator)) {
      row_cnt++;
    }
    cass_iterator_free(load_slice_iterator);
    cass_result_free(load_slice_result);
    (*callback)(row_cnt);
  }

  void LoadSlices(const std::string &database_name,
                  const std::string &table_name, size_t slice_idx,
                  std::function<void(size_t)> callback) {
    if (debug_output) {
      std::cout << "Load: " << table_name << " , at slice: " << slice_idx
                << std::endl;
    }
    if (slice_idx == 0) {
      callback(0);
      return;
    }
    std::unique_lock<std::mutex> lk(mutex_);
    std::string tablename = GetUniformTableName(database_name, table_name);
    auto table_config_it = table_configs_.find(tablename);
    if (table_config_it == table_configs_.end()) {
      std::cerr << "Failed to find table config, tablename: " << tablename
                << std::endl;
      lk.unlock();
      callback(0);
      return;
    }
    const TableConfig &table_config = table_config_it->second;
    const std::string &kv_table_name = table_config.kv_table_name();
    const std::string &keyspace = table_config.keyspace();
    const std::vector<TableSlice> &table_slices = table_config.table_slices();
    if (slice_idx >= table_slices.size()) {
      std::cerr << "Invalid slice index" << std::endl;
      lk.unlock();
      callback(0);
      return;
    }
    const TableSlice &table_slice = table_slices[slice_idx];
    const int32_t partition_id = table_slice.partition_id();
    const std::vector<char> &start_key = table_slice.start_key();
    const std::vector<char> &end_key = table_slice.end_key();
    lk.unlock();

    std::string load_slice_query =
        "SELECT * FROM " + keyspace + "." + kv_table_name +
        " WHERE pk1_ = ? AND pk2_ = ? AND \"___mono_key___\" >= ? AND "
        "\"___mono_key___\" < ? ALLOW FILTERING";
    if (debug_output) {
      std::cout << "load_slice_query: " << load_slice_query << std::endl;
    }
    CassStatement *load_slice_statement =
        cass_statement_new(load_slice_query.c_str(), 4);
    cass_statement_bind_int32(load_slice_statement, 0, partition_id);
    cass_statement_bind_int16(load_slice_statement, 1, -1);
    cass_statement_bind_bytes(
        load_slice_statement, 2,
        reinterpret_cast<const cass_byte_t *>(start_key.data()),
        start_key.size());
    cass_statement_bind_bytes(
        load_slice_statement, 3,
        reinterpret_cast<const cass_byte_t *>(end_key.data()), end_key.size());
    cass_statement_set_paging_size(load_slice_statement, 1000);
    CassFuture *load_slice_result_future =
        cass_session_execute(session_, load_slice_statement);
    cass_future_set_callback(load_slice_result_future, OnLoadSlices, &callback);
    cass_statement_free(load_slice_statement);
    cass_future_free(load_slice_result_future);
  }

  size_t GetSlicesCount(const std::string &database_name,
                        const std::string &table_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::string tablename = GetUniformTableName(database_name, table_name);
    auto table_config_it = table_configs_.find(tablename);
    if (table_config_it == table_configs_.end()) {
      std::cerr << "Failed to find table config" << std::endl;
      return 0;
    }
    return table_config_it->second.table_slices().size();
  }

 private:
  std::mutex mutex_;
  CassCluster *cluster_;
  CassSession *session_;
  std::unordered_map<std::string, TableConfig> table_configs_;
};

class RunningState {
 public:
  explicit RunningState(const int64_t max_flying_req_cnt)
      : stop_flag_(false),
        flying_req_cnt_(0),
        max_flying_req_cnt_(max_flying_req_cnt),
        req_cnt_(0),
        result_cnt_(0) {
    start_time_ = now();
  }

  bool stop_flag() const { return stop_flag_.load(); }
  void set_stop_flag(bool stop_flag) { stop_flag_.store(stop_flag); }
  void inc_flying_req_cnt() { flying_req_cnt_.fetch_add(1); }
  void dec_flying_req_cnt() { flying_req_cnt_.fetch_sub(1); }
  int64_t flying_req_cnt() const { return flying_req_cnt_.load(); }
  bool is_flying_req_cnt_full() {
    return flying_req_cnt_.load() >= max_flying_req_cnt_;
  }
  void inc_req_cnt(int64_t result_cnt) {
    std::lock_guard<std::mutex> lock(mutex_);
    req_cnt_++;
    result_cnt_ += result_cnt;
  }

  std::pair<double, double> qps() {
    std::lock_guard<std::mutex> lock(mutex_);
    auto now_time = now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                        now_time - start_time_)
                        .count();
    if (duration == 0) {
      return {0, 0};
    }
    auto qps = static_cast<double>(req_cnt_) / (duration / 1000.0);
    auto avg_result_cnt =
        static_cast<double>(result_cnt_) / (duration / 1000.0);
    req_cnt_ = 0;
    result_cnt_ = 0;
    start_time_ = now_time;
    return {qps, avg_result_cnt};
  }

 private:
  std::atomic<bool> stop_flag_;
  std::atomic<int64_t> flying_req_cnt_;
  const int64_t max_flying_req_cnt_;
  int64_t req_cnt_;
  int64_t result_cnt_;
  std::chrono::time_point<std::chrono::high_resolution_clock> start_time_;
  std::mutex mutex_;
};

void DumpQPS(RunningState &running_state) {
  while (running_state.stop_flag() == false) {
    std::this_thread::sleep_for(std::chrono::seconds(FLAGS_report_interval));
    auto qps = running_state.qps();

    // print timestamp, qps, avg_result_cnt, flying_req_cnt
    std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(
                     now().time_since_epoch())
                     .count()
              << ", qps: " << qps.first << "/s, select rows: " << qps.second
              << "/s, flying_req_cnt: "
              << running_state.flying_req_cnt() << std::endl;
  }
}

void RunLoadSlicesLoop(const int64_t runner_idx, CassHandler &cass_handler,
                       const std::string database_name,
                       const std::vector<std::string> table_names,
                       RunningState &running_state) {
  std::random_device rd;
  std::default_random_engine generator(rd());
  std::unordered_map<std::string, int64_t> table_slices_map;

  while (running_state.stop_flag() == false) {
    if (running_state.is_flying_req_cnt_full()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
      continue;
    }

    running_state.inc_flying_req_cnt();
    // select table
    std::uniform_int_distribution<int64_t> table_name_dist(
        0, table_names.size() - 1);
    std::string table_name = table_names[table_name_dist(generator)];
    std::string tablename = GetUniformTableName(database_name, table_name);

    // select slice
    int64_t slice_cnt = 0;
    if (table_slices_map.find(tablename) == table_slices_map.end()) {
      slice_cnt = cass_handler.GetSlicesCount(database_name, table_name);
      table_slices_map.emplace(std::make_pair(tablename, slice_cnt));
    } else {
      slice_cnt = table_slices_map[tablename];
    }

    std::uniform_int_distribution<int64_t> slice_dist(0, slice_cnt - 1);
    int64_t slice_idx = table_name_dist(generator);
    // load slice
    cass_handler.LoadSlices(database_name, table_name, slice_idx,
                            [&running_state](size_t result_cnt) {
                              running_state.inc_req_cnt(result_cnt);
                              running_state.dec_flying_req_cnt();
                            });
  }
}

void LoadSlicesStressTest(CassHandler &cass_handler,
                          const std::string &database_name,
                          std::vector<std::string> table_names,
                          RunningState &running_state) {
  std::vector<std::thread> threads;
  for (int64_t i = 0; i < FLAGS_num_threads; i++) {
    threads.emplace_back(std::thread(RunLoadSlicesLoop, i,
                                     std::ref(cass_handler), database_name,
                                     table_names, std::ref(running_state)));
  }
  std::thread qps_thread(DumpQPS, std::ref(running_state));

  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_test_duration));
  running_state.set_stop_flag(true);
  for (auto &thread : threads) {
    thread.join();
  }
  qps_thread.join();
}

int main(int argc, char *argv[]) {
  GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
  std::cout << "cass_endpoints: " << FLAGS_cass_endpoints << std::endl;
  std::cout << "cass_port: " << FLAGS_cass_port << std::endl;
  std::cout << "cass_keyspace: " << FLAGS_cass_keyspace << std::endl;
  std::cout << "cass_username: " << FLAGS_cass_username << std::endl;
  std::cout << "cass_password: " << FLAGS_cass_password << std::endl;
  std::cout << "cass_queue_size_io: " << FLAGS_cass_queue_size_io << std::endl;
  std::cout << "cass_num_threads_io: " << FLAGS_cass_num_threads_io
            << std::endl;
  std::cout << "cass_core_connections_per_host: "
            << FLAGS_cass_core_connections_per_host << std::endl;
  std::cout << "mono_database_name: " << FLAGS_mono_database_name << std::endl;
  std::cout << "mono_table_names: " << FLAGS_mono_table_names << std::endl;
  std::cout << "mono_table_columns: " << FLAGS_mono_table_columns << std::endl;
  std::cout << "num_threads: " << FLAGS_num_threads << std::endl;
  std::cout << "max_flying_req_num: " << FLAGS_max_flying_req_num << std::endl;
  std::cout << "test_duration: " << FLAGS_test_duration << std::endl;

  CassHandler cass_handler(FLAGS_cass_endpoints, FLAGS_cass_port,
                           FLAGS_cass_username, FLAGS_cass_password,
                           FLAGS_cass_queue_size_io, FLAGS_cass_num_threads_io,
                           FLAGS_cass_core_connections_per_host);
  if (!cass_handler.Connect()) {
    std::cerr << "Failed to connect to cassandra" << std::endl;
    return -1;
  }

  std::cout << "Start loading table config..." << std::endl;
  std::vector<std::string> table_names =
      SplitString(FLAGS_mono_table_names, ',');
  std::vector<std::string> table_columns =
      SplitString(FLAGS_mono_table_columns, ',');
  std::vector<std::string> table_column_types =
      SplitString(FLAGS_mono_table_column_types, ',');
  for (auto &table_name : table_names) {
    std::cout << "Loading table config for table: " << table_name << std::endl;
    if (!cass_handler.LoadTableConfig(FLAGS_cass_keyspace,
                                      FLAGS_mono_database_name, table_name,
                                      table_columns, table_column_types)) {
      std::cerr << "Failed to load table config" << std::endl;
      return -1;
    }
  }

  std::cout << "Start test running..." << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(5));
  RunningState running_state(FLAGS_max_flying_req_num);
  LoadSlicesStressTest(cass_handler, FLAGS_mono_database_name, table_names,
                       running_state);
}
