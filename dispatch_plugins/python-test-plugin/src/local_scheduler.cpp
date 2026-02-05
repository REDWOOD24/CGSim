// local_scheduler.cpp
#include "local_scheduler.h"

#include <chrono>
#include <stdexcept>
#include <unordered_map>
#include <vector>
#include <random>
#include <string>

static long long storage_needed(const std::unordered_map<std::string, long long>& files)
{
  long long sum = 0;
  for (const auto& kv : files)
    sum += kv.second;
  return sum;
}

LOCAL_SCHEDULER::LOCAL_SCHEDULER(std::string defaultAlgorithm)
  : defaultAlgorithm_(std::move(defaultAlgorithm))
{
  const auto seed =
      (uint64_t)std::chrono::high_resolution_clock::now().time_since_epoch().count();
  rng_.seed(seed);
}

void LOCAL_SCHEDULER::assignJob(Job* job, const std::string& schedulerAlgorithm)
{
  if (!job) return;
  if (job->cores <= 0) job->cores = job->core_count;

  if (job->comp_site.empty()) {
    job->status = "pending";
    return;
  }

  const std::string alg = schedulerAlgorithm.empty() ? defaultAlgorithm_ : schedulerAlgorithm;

  sg4::NetZone* root = getRoot_();
  sg4::NetZone* site = findSite_(root, job->comp_site);
  if (!site) {
    job->status = "pending";
    return;
  }

  sg4::Host* cpu = nullptr;
  if (alg == "random") {
    cpu = pickRandomCpuInSite_(site, job);
  } else {
    job->status = "pending";
    return;
  }

  if (!cpu) {
    job->status = "pending";
    return;
  }

  applyAssignment_(job, site, cpu);
}

sg4::NetZone* LOCAL_SCHEDULER::getRoot_() const
{
  auto* engine = sg4::Engine::get_instance();
  if (!engine) throw std::runtime_error("LOCAL_SCHEDULER: Engine is null");

  auto* root = engine->get_netzone_root();
  if (!root) throw std::runtime_error("LOCAL_SCHEDULER: root netzone is null");

  return root;
}

sg4::NetZone* LOCAL_SCHEDULER::findSite_(sg4::NetZone* root, const std::string& siteName) const
{
  for (auto* z : root->get_children()) {
    if (!z) continue;
    if (z->get_name() == "JOB-SERVER") continue;
    if (z->get_name() == siteName) return z;
  }
  return nullptr;
}

bool LOCAL_SCHEDULER::isComputeHost_(sg4::Host* h) const
{
  return h && (h->get_name().find("_communication") == std::string::npos);
}

bool LOCAL_SCHEDULER::hasResources_(sg4::Host* cpu, const Job* j) const
{
  if (!cpu || !j) return false;

  auto* ext = cpu->extension<HostExtensions>();
  if (!ext) return false;
  if (ext->get_cores_available() < j->cores) return false;

  const std::string site = cpu->get_englobing_zone()->get_name();
  if (CGSim::FileManager::request_remaining_site_storage(site) < storage_needed(j->output_files))
    return false;

  return true;
}

sg4::Disk* LOCAL_SCHEDULER::pickDisk_(sg4::Host* cpu) const
{
  if (!cpu) return nullptr;
  const auto& disks = cpu->get_disks();
  if (disks.empty()) return nullptr;
  return disks[0];
}

sg4::Host* LOCAL_SCHEDULER::pickRandomCpuInSite_(sg4::NetZone* site, const Job* j)
{
  std::vector<sg4::Host*> eligible;

  for (auto* h : site->get_all_hosts()) {
    if (!isComputeHost_(h)) continue;
    if (!hasResources_(h, j)) continue;
    eligible.push_back(h);
  }

  if (eligible.empty()) return nullptr;

  std::uniform_int_distribution<size_t> dist(0, eligible.size() - 1);
  return eligible[dist(rng_)];
}

void LOCAL_SCHEDULER::applyAssignment_(Job* job, sg4::NetZone* site, sg4::Host* cpu)
{
  if (!job || !site || !cpu) {
    if (job) job->status = "pending";
    return;
  }

  if (job->cores <= 0)
    job->cores = job->core_count;

  // --- SAFE GFLOPS property read (Simple dispatcher style, but guarded) ---
  const char* gflops_c = site->get_property("GFLOPS");
  if (!gflops_c || !*gflops_c) {
    // fallback: use cpu speed if property missing (prevents nullptr crash)
    job->flops = (long long)(
        (double)cpu->get_speed() *
        (double)job->cpu_consumption_time *
        (double)job->cores
    );
  } else {
    // matches your simple dispatcher formula
    job->flops = (long long)(
        std::stoll(gflops_c) *
        (double)job->cpu_consumption_time *
        (double)job->cores
    );
  }

  auto* ext = cpu->extension<HostExtensions>();
  if (!ext) {
    job->status = "pending";
    return;
  }

  // IMPORTANT: your CGSim HostExtensions has no set_cores_available(), so don't decrement here.

  // Disk metadata (match SIMPLE_DISPATCHER)
  if (!cpu->get_disks().empty()) {
    auto* d = cpu->get_disks()[0];
    job->disk          = d->get_name();
    job->disk_read_bw  = d->get_read_bandwidth();
    job->disk_write_bw = d->get_write_bandwidth();
  } else {
    job->disk.clear();
    job->disk_read_bw  = 0.0;
    job->disk_write_bw = 0.0;
  }

  job->comp_site       = site->get_name();
  job->comp_host       = cpu->get_name();
  job->comp_host_speed = cpu->get_speed();
  job->status          = "assigned";
}
