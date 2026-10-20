#include "scoring_output.h"

#include <filesystem>

void ScoringOutput::initialize() {
  if (initialized) return;
  std::string file_name = CGSim::GlobalManagers::get_resource_manager()->get_custom_parameter("output_file");
  if (std::filesystem::exists(file_name)) std::filesystem::remove(file_name);

  if (sqlite3_open(file_name.c_str(), &db) != SQLITE_OK) {
    throw std::invalid_argument("SQLite file " + file_name + " cannot be opened.");
  }
  if (SQLITE_OK != sqlite3_exec(db, "PRAGMA journal_mode=WAL;", nullptr, 0, nullptr)) {
    throw std::runtime_error("Failed to set database connection in WAL mode.");
  }
  initialized = true;
  createTables();
  writeRunMetadata();
}

void ScoringOutput::createTables() {
  const char* create_results =
      "CREATE TABLE IF NOT EXISTS JOB_RESULTS ("
      "JOB_ID TEXT PRIMARY KEY, "
      "POLICY_NAME TEXT NOT NULL, "
      "ACTIVE_PHASE TEXT NOT NULL, "
      "ASSIGNED_SITE TEXT NOT NULL, "
      "ASSIGNED_CPU TEXT NOT NULL, "
      "HISTORICAL_SITE TEXT NOT NULL, "
      "CORES INTEGER NOT NULL, "
      "CREATION_TIME REAL NOT NULL, "
      "SUBMISSION_TIME REAL NOT NULL, "
      "RESOURCE_WAITING_QUEUE_TIME REAL NOT NULL, "
      "FILE_TRANSFER_QUEUE_TIME REAL NOT NULL, "
      "COMPLETION_TIME REAL NOT NULL, "
      "RETRIES INTEGER NOT NULL, "
      "STATUS TEXT NOT NULL"
      ");";

  const char* create_metadata =
      "CREATE TABLE IF NOT EXISTS RUN_METADATA (KEY TEXT PRIMARY KEY, VALUE TEXT);";

  const char* create_switches =
      "CREATE TABLE IF NOT EXISTS POLICY_SWITCHES ("
      "_ID INTEGER PRIMARY KEY AUTOINCREMENT, "
      "SWITCH_TIME REAL NOT NULL, "
      "FROM_PHASE TEXT NOT NULL, "
      "TO_PHASE TEXT NOT NULL"
      ");";

  const char* create_events =
      "CREATE TABLE IF NOT EXISTS EVENTS ("
      "_ID INTEGER PRIMARY KEY AUTOINCREMENT, "
      "JOB_ID TEXT NOT NULL, "
      "CPU_NAME TEXT NOT NULL, "
      "STATE TEXT NOT NULL, "
      "TIMESTAMP REAL NOT NULL, "
      "SITE TEXT NOT NULL, "
      "AVAILABLE_SITE_CORES INTEGER NOT NULL, "
      "SITE_PENDING_JOBS INTEGER NOT NULL, "
      "SITE_RUNNING_JOBS INTEGER NOT NULL, "
      "SITE_FINISHED_JOBS INTEGER NOT NULL, "
      "SITE_FAILED_JOBS INTEGER NOT NULL"
      ");";

  char* errmsg = nullptr;
  for (const char* stmt : {create_results, create_metadata, create_switches, create_events}) {
    if (sqlite3_exec(db, stmt, nullptr, nullptr, &errmsg) != SQLITE_OK) {
      std::string msg = errmsg ? errmsg : sqlite3_errmsg(db);
      sqlite3_free(errmsg);
      throw std::runtime_error("Database table creation failed: " + msg);
    }
  }
}

void ScoringOutput::writeRunMetadata() {
  auto* rm = CGSim::GlobalManagers::get_resource_manager();
  auto put = [&](const std::string& key, const std::string& value) {
    const char* sql = "INSERT OR REPLACE INTO RUN_METADATA (KEY, VALUE) VALUES (?, ?);";
    sqlite3_stmt* stmt = nullptr;
    sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr);
    sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, value.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
  };
  put("policy_name", spec_.policy_name);
  put("dispatch_mode", spec_.active_spec().dispatch_mode == ScoringPolicy::DispatchMode::Replay ? "replay" : "score");
  put("is_adaptive", spec_.is_adaptive() ? "true" : "false");
  put("phase_count", std::to_string(spec_.phases.size()));
  try {
    put("jobs_file", rm->get_custom_parameter("jobs_file"));
  } catch (const std::exception&) {
  }
}

long long ScoringOutput::input_files_bytes(CGSim::Job* job) {
  long long total_bytes = 0;
  auto* fm = CGSim::GlobalManagers::get_file_manager();
  for (const auto& filename : job->get_input_files()) total_bytes += fm->request_file_size(filename);
  return total_bytes;
}

long long ScoringOutput::output_files_bytes(CGSim::Job* job) {
  long long total_bytes = 0;
  for (const auto& [filename, filesize] : job->get_output_files())
    total_bytes += CGSim::Utilities::parse_units_size(filesize);
  return total_bytes;
}

void ScoringOutput::insertJobResult(CGSim::Job* job, const std::string& status) {
  std::string historical_site;
  try {
    historical_site = job->get_property("historical_site");
  } catch (const std::exception&) {
    historical_site = "";
  }

  // The phase active when this job's site was actually decided (set by
  // ScoringDispatcher::assignJob) -- not necessarily spec_.active_phase_name()
  // right now, since an adaptive policy may have switched phases since.
  // Jobs that never got a site (e.g. failed while still globally pending)
  // never had this set.
  std::string active_phase;
  try {
    active_phase = job->get_property("assigned_under_phase");
  } catch (const std::exception&) {
    active_phase = "";
  }

  const char* sql =
      "INSERT OR REPLACE INTO JOB_RESULTS ("
      "JOB_ID, POLICY_NAME, ACTIVE_PHASE, ASSIGNED_SITE, ASSIGNED_CPU, HISTORICAL_SITE, CORES, "
      "CREATION_TIME, SUBMISSION_TIME, RESOURCE_WAITING_QUEUE_TIME, FILE_TRANSFER_QUEUE_TIME, "
      "COMPLETION_TIME, RETRIES, STATUS"
      ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

  sqlite3_stmt* stmt = nullptr;
  if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
    throw std::runtime_error(std::string("SQLite prepare failed: ") + sqlite3_errmsg(db));
  }
  sqlite3_bind_text(stmt, 1, job->get_id().c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt, 2, spec_.policy_name.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt, 3, active_phase.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt, 4, job->get_site().c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt, 5, job->get_cpu().c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt, 6, historical_site.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int(stmt, 7, job->get_cores());
  sqlite3_bind_double(stmt, 8, job->get_creation_time());
  sqlite3_bind_double(stmt, 9, job->get_submission_time());
  sqlite3_bind_double(stmt, 10, job->get_resource_waiting_queue_time());
  sqlite3_bind_double(stmt, 11, job->get_file_transfer_queue_time());
  sqlite3_bind_double(stmt, 12, sg4::Engine::get_clock());
  sqlite3_bind_int(stmt, 13, job->get_retries());
  sqlite3_bind_text(stmt, 14, status.c_str(), -1, SQLITE_TRANSIENT);

  if (sqlite3_step(stmt) != SQLITE_DONE) {
    std::string err = sqlite3_errmsg(db);
    sqlite3_finalize(stmt);
    throw std::runtime_error("SQLite step failed: " + err);
  }
  sqlite3_finalize(stmt);
}

void ScoringOutput::onJobFinish(CGSim::Job* job) { insertJobResult(job, "finished"); }

void ScoringOutput::onJobFailure(CGSim::Job* job) { insertJobResult(job, "failed"); }

void ScoringOutput::insertPolicySwitch(const std::string& from_phase, const std::string& to_phase, double sim_time) {
  const char* sql = "INSERT INTO POLICY_SWITCHES (SWITCH_TIME, FROM_PHASE, TO_PHASE) VALUES (?, ?, ?);";
  sqlite3_stmt* stmt = nullptr;
  if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) return;
  sqlite3_bind_double(stmt, 1, sim_time);
  sqlite3_bind_text(stmt, 2, from_phase.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt, 3, to_phase.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_step(stmt);
  sqlite3_finalize(stmt);
}

void ScoringOutput::insertDebugEvent(CGSim::Job* job) {
  auto* site = CGSim::GlobalManagers::get_resource_manager()->get_site(job->get_site());
  const char* sql =
      "INSERT INTO EVENTS (JOB_ID, CPU_NAME, STATE, TIMESTAMP, SITE, AVAILABLE_SITE_CORES, "
      "SITE_PENDING_JOBS, SITE_RUNNING_JOBS, SITE_FINISHED_JOBS, SITE_FAILED_JOBS) "
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
  sqlite3_stmt* stmt = nullptr;
  if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) return;
  sqlite3_bind_text(stmt, 1, job->get_id().c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt, 2, job->get_cpu().c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt, 3, job->get_status().c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_double(stmt, 4, sg4::Engine::get_clock());
  sqlite3_bind_text(stmt, 5, job->get_site().c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int64(stmt, 6, site->get_number_of_total_cores() - site->get_number_of_used_cores());
  sqlite3_bind_int64(stmt, 7, site->get_pending_jobs().size());
  sqlite3_bind_int64(stmt, 8, site->get_running_jobs().size());
  sqlite3_bind_int64(stmt, 9, site->get_finished_jobs().size());
  sqlite3_bind_int64(stmt, 10, site->get_failed_jobs().size());
  sqlite3_step(stmt);
  sqlite3_finalize(stmt);
}

void ScoringOutput::onJobStatusChange(CGSim::Job* job) {
  if (!spec_.active_spec().emit_debug_events) return;
  if (job->get_site().empty()) return;  // no site yet (still global-pending); nothing to log
  insertDebugEvent(job);
}
