#include <reduct/client.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <regex>
#include <thread>

constexpr std::string_view kReductStorageUrl = "http://127.0.0.1:8383";
constexpr std::string_view kWatchedPath = "./";

namespace fs = std::filesystem;

int main() {
  using ReductClient = reduct::IClient;
  using ReductBucket = reduct::IBucket;

  auto client = ReductClient::Build(kReductStorageUrl);

  auto [bucket, err] =
      client->GetOrCreateBucket("watched_files", ReductBucket::Settings{
                                                     .quota_type = ReductBucket::QuotaType::kFifo,
                                                     .quota_size = 100'000'000,  // 100Mb
                                                 });
  if (err) {
    std::cerr << "Failed to create bucket" << err << std::endl;
    return -1;
  }

  std::cout << "Create bucket" << std::endl;

  std::map<std::string, fs::file_time_type> file_timestamp_map;
  for (;;) {
    for (auto& file : fs::directory_iterator(kWatchedPath)) {
      bool is_changed = false;
      // check only files
      if (!fs::is_regular_file(file)) {
        continue;
      }

      const auto filename = file.path().filename().string();
      auto ts = fs::last_write_time(file);

      if (file_timestamp_map.contains(filename)) {
        auto& last_ts = file_timestamp_map[filename];
        if (ts != last_ts) {
          is_changed = true;
        }
        last_ts = ts;
      } else {
        file_timestamp_map[filename] = ts;
        is_changed = true;
      }

      if (!is_changed) {
        continue;
      }

      std::string alias = filename;
      std::regex_replace(alias.begin(), filename.begin(), filename.end(), std::regex("\\."),
                         "_");  // we use filename as an entyr name. It can't contain dots.
      std::cout << "`" << filename << "` is changed. Storing as `" << alias << "` ";

      std::ifstream changed_file(file.path());
      if (!changed_file) {
        std::cerr << "Failed open file";
        return -1;
      }

      auto file_size = fs::file_size(file);

      auto write_err =
          bucket->Write(alias, std::chrono::file_clock::to_sys(ts),
                        [file_size, &changed_file](ReductBucket::WritableRecord* rec) {
                          rec->Write(file_size, [&](size_t offest, size_t size) {
                            std::string buffer;
                            buffer.resize(size);
                            changed_file.read(buffer.data(), size);
                            std::cout << "." << std::flush;
                            return std::pair{offest + size <= file_size, buffer};
                          });
                        });

      if (write_err) {
        std::cout << " Err:" << write_err << std::endl;
      } else {
        std::cout << " OK (" << file_size / 1024 << " kB)" << std::endl;
      }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  return 0;
}
