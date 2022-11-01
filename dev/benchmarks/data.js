window.BENCHMARK_DATA = {
  "lastUpdate": 1667274362922,
  "repoUrl": "https://github.com/lucassong-mh/ngo",
  "entries": {
    "FIO Benchmark": [
      {
        "commit": {
          "author": {
            "email": "1498430017@qq.com",
            "name": "Song Shaowei",
            "username": "lucassong-mh"
          },
          "committer": {
            "email": "1498430017@qq.com",
            "name": "Song Shaowei",
            "username": "lucassong-mh"
          },
          "distinct": true,
          "id": "b2bd1e92223d8d0be410b76c37bbfba3f9f706b4",
          "message": "[benchmark ci] Add fio to benchmark ci",
          "timestamp": "2022-11-01T11:26:45+08:00",
          "tree_id": "c239fa04ae74d9cf66f7c00c5d1ba006d749c9b9",
          "url": "https://github.com/lucassong-mh/ngo/commit/b2bd1e92223d8d0be410b76c37bbfba3f9f706b4"
        },
        "date": 1667274361285,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "Sequential Write Throughput",
            "value": 30.9,
            "unit": "MiB/s",
            "extra": "seqwrite"
          },
          {
            "name": "Random Write Throughput",
            "value": 24.8,
            "unit": "MiB/s",
            "extra": "randwrite"
          },
          {
            "name": "Sequential Read Throughput",
            "value": 214,
            "unit": "MiB/s",
            "extra": "seqread"
          },
          {
            "name": "Random Read Throughput",
            "value": 188,
            "unit": "MiB/s",
            "extra": "randread"
          }
        ]
      }
    ]
  }
}