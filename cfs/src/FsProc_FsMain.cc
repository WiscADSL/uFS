#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>

#include <experimental/filesystem>
#include <iostream>
#include <memory>

#include "FsLibShared.h"
#include "FsProc_Fs.h"
#include "config4cpp/Configuration.h"
#include "spdlog/spdlog.h"
#include "typedefs.h"

// global fs object
FsProc *gFsProcPtr = nullptr;
static const char *gFspConfigFname = nullptr;

void handle_sigint(int sig);

// Main function of FSP
// @numworkers: number of thread of FSP
// @numAppProc: number of application process it is going to handle (will
// @provision memory ring accordingly)
// @shmBaseOffset: if specified, the intention is to run multiple FSP
//   processes at a time, then each use different shmkey group. E.g., FSP1:
//   Base+1+ProcID, FSP2: Base+2+ProcID
// @exitSignalFileName: file name for FSP to monitor and thus decide to exit.
// @configFileName: the config file used to initialize SPDK dev only, all the
//   parameter will be used as SPDK options
int fsMain(int numWorkers, int numAppProc, std::vector<int> &shmBaseOffsets,
           const char *exitSignalFileName, const char *configFileName,
           bool isSpdk, std::vector<int> &workerCores) {
#if CFS_JOURNAL(OFF)
  fprintf(stdout, "Journal is disabled\n");
#else
  fprintf(stdout, "Journal is enabled\n");
#endif
#ifdef NDEBUG
  fprintf(stdout, "NDEBUG defined\n");
#else
  fprintf(stdout, "NDEBUG not defined\n");
#endif
  macro_print(NMEM_DATA_BLOCK);
  CurBlkDev *devPtr = nullptr;
  SPDLOG_INFO("fsMain");
  std::vector<CurBlkDev *> devVec;
  // NOTE: devPtr is used as a global variable previously
  if (configFileName == nullptr) {
    if (isSpdk) {
      devPtr = new CurBlkDev("", DEV_SIZE / BSIZE, BSIZE);
    } else {
      devPtr = new CurBlkDev(BLK_DEV_POSIX_FILE_NAME, DEV_SIZE / BSIZE, BSIZE);
    }
    devPtr->updateWorkerNum(numWorkers);
    devVec.push_back(devPtr);
  } else {
    std::string configNameStr(configFileName);
    if (isSpdk) {
      devPtr = new CurBlkDev("", DEV_SIZE / BSIZE, BSIZE, configNameStr);

    } else {
      devPtr = new CurBlkDev(BLK_DEV_POSIX_FILE_NAME, DEV_SIZE / BSIZE, BSIZE,
                             configFileName);
    }
    devPtr->updateWorkerNum(numWorkers);
    // For both BlkDevSpdk and BlkDevPosix, we let all threads share the same
    // virtual block device
    for (int i = 0; i < numWorkers; i++) {
      devVec.push_back(devPtr);
    }
  }
  // devVec[0]->enableReportStats(true);
  gFsProcPtr = new FsProc(numWorkers, numAppProc, exitSignalFileName);
  // std::cout << "len devVec:" << devVec.size() << std::endl;

  if (gFspConfigFname != nullptr) {
    fprintf(stderr, "setConfigFname\n");
    gFsProcPtr->setConfigFname(gFspConfigFname);
  }

#ifdef FSP_ENABLE_ALLOC_READ_RA
  std::cout << "READAHEAD raNumBlocks:" << gFsProcPtr->getRaNumBlock()
            << std::endl;
#endif

  std::cout << "ServerCorePolicy:" << gFsProcPtr->getServerCorePolicyNo()
            << std::endl;
  std::cout << "lb_cgst_ql:" << gFsProcPtr->GetLbCgstQl() << std::endl;
  std::cout << "nc_percore_ut:" << gFsProcPtr->GetNcPerCoreUt() << std::endl;

  signal(SIGINT, handle_sigint);
  // start workers
  gFsProcPtr->startWorkers(shmBaseOffsets, devVec, workerCores);
  return 0;
}

void usage(char **argv) {
  std::cerr << "Usage:" << argv[0]
            << " <numWorkers> <numAppProc> <shmBaseOffset> <exitFileName> "
               "<devConfigName> <workerCores> [FspConfigName]\n"
            << "  numWorkers: number of threads for running FSP\n"
            << "  numAppProc: number of application's pre-registered apps\n"
            << "  shnBaseOffset: offset of shmkey compared to FS_SHM_KEY_BASE\n"
            << "    - e.g., 1,11 (for numWorkers=2)\n"
            << "  exitFileName: a file that once show up <=> fs exit\n"
            << "  [devConfigName] REQUIRED when numWorker>1\n"
            << "  workerCores: comma separated list of cores to pin workers\n"
            << "  ENVIRONMENTAL VARIABLES: \n"
            << "   READY_FILE_NAME : write to filepath when ready\n";
}

void check_root() {
  int uid = getuid();
  if (uid != 0) {
    printOnErrorExitSymbol();
    std::cerr << "Error, must be invoked in root mode. \nExit ......\n";
    exit(1);
  }
}

void logFeatureMacros() {
#if CFS_JOURNAL(NO_JOURNAL)
  SPDLOG_INFO("CFS_JOURNAL(NO_JOURNAL) = True");
#endif

#if CFS_JOURNAL(ON)
  SPDLOG_INFO("CFS_JOURNAL(ON) = True");
#endif

#if CFS_JOURNAL(LOCAL_JOURNAL)
  SPDLOG_INFO("CFS_JOURNAL(LOCAL_JOURNAL) = True");
#endif

#if CFS_JOURNAL(GLOBAL_JOURNAL)
  SPDLOG_INFO("CFS_JOURNAL(GLOBAL_JOURNAL) = True");
#endif

#if CFS_JOURNAL(PERF_METRICS)
  SPDLOG_INFO("CFS_JOURNAL(PERF_METRICS) = True");
#endif

#if !CFS_JOURNAL(CHECKPOINTING)
  // This should only be used when testing writes where you don't want
  // checkpointing to be measured. It will fail offlineCheckpointer so it cannot
  // be used accross multiple runs of fsp. Mkfs must be called after this run of
  // fsp.
  SPDLOG_WARN("CFS_JOURNAL(CHECKPOINTING) = False");
#endif
}

static const int gHostNameLen = 512;
static char gHostName[gHostNameLen];
int main(int argc, char **argv) {
  check_root();

  google::InitGoogleLogging(argv[0]);

  // print hostname
  gethostname(gHostName, gHostNameLen);
  std::cout << argv[0] << " started in host:" << gHostName << "\n";

  // Print macro configuration here
  bool isSpdk = false;
#ifdef USE_SPDK
  isSpdk = true;
#endif

#ifndef NONE_MT_LOCK
  std::cout << "NONE_MT_LOC - OFF" << std::endl;
#else
  std::cout << "NONE_MT_LOC - ON" << std::endl;
#endif

#ifndef MIMIC_FSP_ZC
  std::cout << "MIMIC_FSP_ZC - OFF" << std::endl;
#else
  std::cout << "MIMIC_FSP_ZC - ON" << std::endl;
#endif

#if (FS_LIB_USE_APP_CACHE)
  std::cout << "FS_LIB_USE_APP_CACHE - ON" << std::endl;
#else
  std::cout << "FS_LIB_USE_APP_CACHE - OFF" << std::endl;
#endif

#ifdef FSP_ENABLE_ALLOC_READ_RA
  std::cout << "FS_ENABLE_ALLOC_READ_RA - ON" << std::endl;
#else
  std::cout << "FS_ENABLE_ALLOC_READ_RA - OFF" << std::endl;
#endif

#ifdef USE_RAM_DEV
  std::cout << "USE_RAM_DEV - ON" << std::endl;
#else
  std::cout << "USE_RAM_DEV - OFF" << std::endl;
#endif

  logFeatureMacros();
  std::vector<int> shmBaseOffsetVec = {0};
  std::vector<int> workerCores;  // empty by default
  if (argc == 3) {
    // By Default no shmBaseOffset specified, this is to be compatible with old
    // experiments.
    fsMain(std::stoi(std::string(argv[1])), std::stoi(std::string(argv[2])),
           /*shmBaseOffset*/ shmBaseOffsetVec, /*exitSignalFileName*/ nullptr,
           /*configFileName*/ nullptr, isSpdk, workerCores);
  } else if (argc == 5) {
    shmBaseOffsetVec[0] = std::stoi(std::string(argv[3]));
    fsMain(std::stoi(std::string(argv[1])), std::stoi(std::string(argv[2])),
           shmBaseOffsetVec, argv[4], nullptr, isSpdk, workerCores);
  } else if (argc >= 6) {
    // NOTE: now this argument list is preferable. (2020/07/29)
    auto numWorkers = std::stoi(std::string(argv[1]));
    auto strs = splitStr(std::string(argv[3]), ',');
    fprintf(stdout, "======= FSP: config file (%s), shmOffsetList:%s =======\n",
            argv[5], argv[3]);
    shmBaseOffsetVec.clear();
    for (const auto &str : strs) shmBaseOffsetVec.push_back(std::stoi(str));
    if (shmBaseOffsetVec.size() != (unsigned)numWorkers) {
      fprintf(stderr, "numWorkers should match shmBaseOffset. Exit...\n");
      exit(1);
    }

    if (argc == 7 || argc == 8) {
      // last argument is a comma separated list of cores to pin workers on
      // TODO convert code to use getopt - will be much cleaner
      // e.g. cfs/test/shmipc/client.c and cfs/test/shmipc/myargs.h
      // NOTE: these changes would also require changing all benchmark
      // scripts and experiments to make sure they work with the new format.

      auto strs = splitStr(std::string(argv[6]), ',');
      for (const auto &str : strs) workerCores.push_back(std::stoi(str));
      if (workerCores.size() != (unsigned)numWorkers) {
        fprintf(stderr, "numWorkers should match workerCores. Exit...\n");
        exit(1);
      }
    }

    if (argc == 8 && std::experimental::filesystem::exists(argv[7])) {
      // optional argument
      // use this to pass in a configuration file to configure FSP behavior
      // mostly specify the policy
      gFspConfigFname = argv[7];
      SPDLOG_INFO("FSP configuration file is set to :{}", gFspConfigFname);
    }
    fsMain(numWorkers, std::stoi(std::string(argv[2])), shmBaseOffsetVec,
           argv[4], argv[5], isSpdk, workerCores);
  } else {
    usage(argv);
    return -1;
  }
  return 0;
}

void handle_sigint(int sig) {
  gFsProcPtr->stop();
  // delete gFsProcPtr;
  // exit(0);
}
