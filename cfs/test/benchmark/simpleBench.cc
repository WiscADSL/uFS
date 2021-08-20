
#include <iostream>
#include <chrono>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <mutex>
#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>
#include <cassert>
#include <random>
#include <functional>

#include "param.h"
#include "fsapi.h"


//uint32_t maxFileSize = 2 * 1024 * 1024;
uint32_t maxFileSize = 1024 * 1024 * 1024;

std::string binName;
static int pIdx;
static int numThreads;
std::atomic_bool workerRun(true);


std::mutex lmutex;

#ifdef USE_SPDK
std::string fileDir = std::string("/");
#else
std::string fileDir = std::string("/mnt/optanemnt/data/");
#endif

enum WorkloadType {
    RAND_READ = 1, RAND_WRITE = 2,
    SEQ_READ = 3, SEQ_WRITE = 4,
};

class BenchWorker{
    std::chrono::steady_clock::time_point startTs;
    std::chrono::steady_clock::time_point finishTs;
    uint64_t reqFinished;

public:
    void operator() (int wid, bool timeout, int endCond, WorkloadType type, uint32_t reqByte) {
        reqFinished = 0;
        char *userBuf = new char[reqByte];
        // random generator
        auto seed = std::chrono::high_resolution_clock::now().time_since_epoch().count();
        auto dice_rand = std::bind(std::uniform_int_distribution<uint32_t>(0, maxFileSize-2*reqByte),
                                   std::mt19937(seed));
        std::string pathStr = fileDir + std::string("t");
        pathStr += std::to_string(wid);
        std::cout << "open file" << pathStr << std::endl;
        // do work
        switch (type) {
            case RAND_READ: {
                ssize_t rc;
                off_t curOff;
                int fd = fs_open(pathStr.c_str(), O_RDWR, 0);
                if(fd < 0) {
                    std::cerr << "ERROR cannot open file " << pathStr.c_str() << std::endl;
                    return;
                }
                startTs = std::chrono::steady_clock::now();
                while (workerRun) {
                    if (!timeout && reqFinished >= endCond) {
                        break;
                    }
                    curOff = dice_rand();
                    rc = fs_pread(fd, userBuf, reqByte, curOff);
                    assert(rc == reqByte);
                    reqFinished++;
                }
                finishTs = std::chrono::steady_clock::now();
                break;
            }
            case RAND_WRITE:
                // not supported
                break;
            case SEQ_READ: {
                int fd = fs_open(pathStr.c_str(), O_RDWR, 0);
                if(fd < 0) {
                    std::cerr << "ERROR cannot open file " << pathStr.c_str() << std::endl;
                    return;
                }
                uint64_t numReqs = maxFileSize / reqByte;
                ssize_t rc;
                startTs = std::chrono::steady_clock::now();
                for (uint i = 0; i < numReqs; i++) {
                    //rc = fs_read(fd, userBuf, reqByte);
                    rc = fs_pread(fd, userBuf, reqByte, i * reqByte);
                    assert(rc == reqByte);
                }
                reqFinished = numReqs;
                finishTs = std::chrono::steady_clock::now();
                break;
            }
            case SEQ_WRITE:
                // not supported
                break;
            default:
                std::cerr << "unkown workload" << std::endl;
                throw;
        } // end of work cases
        std::lock_guard<std::mutex> lock(lmutex);
        {
            // report result
            double durUs = std::chrono::duration_cast<std::chrono::microseconds>(finishTs - startTs).count();
            std::cout << "wid:" << wid << " reqFinished:" << reqFinished << " timeUs:" << durUs << std::endl;
            // clean up
        }
        delete(userBuf);
    }
};


void printUsage() {
    std::cout
            << "Usage: " << binName
            << " <PID> "
            << " <numThreads>"
            << " <T|N+secs|numTotal>"
            << " <workload ID> <reqByte> " << std::endl;
    std::cout
            << "---------------------------------------------------------------------\n"
            << "<PID>: index of process (from 0, used to get shmid for cfs\n"
            << "<numThreas>: number of threads\n"
            << "<T|N>: T means benchmark until time is used up (in seconds), "
               "N means total number of requests (all the threads)\n"
            << "<workload id>: 1:random read, 2:random write, 3:seq read, 4:seq write.\n"
            << "<reqByte>: size in Bytes of each request\n";
}

void benchMain(std::vector<std::string> & cmdStrVec) {
    std::cout << fileDir << std::endl;
    if(cmdStrVec.size() != 5) {
        printUsage();
        exit(1);
    }
    // process params
    int vecIdx = 0;
    pIdx = std::stoi(cmdStrVec[vecIdx++]);
    numThreads = std::stoi(cmdStrVec[vecIdx++]);
    bool timeOut;
    if(cmdStrVec[vecIdx][0] == 'T') {
        timeOut = true;
    }else if(cmdStrVec[vecIdx][0] == 'N') {
        timeOut = false;
    }else{
        throw;
    }
    int endCond = std::stoi((cmdStrVec[vecIdx++]).substr(1));
    WorkloadType workloadType = (WorkloadType) std::stoi(cmdStrVec[vecIdx++]);
    int reqByte = std::stoi(cmdStrVec[vecIdx++]);
    std::cout << "pIdx:" << pIdx << " numThreads:" << numThreads
              << " timeOut:" << timeOut << " endCond:" << endCond << " workload:" << workloadType
              << " reqByte:" << reqByte << std::endl;

    if(!timeOut) {
        endCond = endCond / numThreads;
    }

    // init fs
    fs_init(FS_SHM_KEY_BASE + pIdx);

    // init workers
    std::thread *workers = new std::thread[numThreads];
    for(int i = 0; i < numThreads; i++) {
        // assume pIdx is a virtual pid starting from 0
        // when using this benchmark, each process has the same # of threads (numthreads)
        // to make sure each thread access unique file, i+pIdx * numthreads will generate
        // filename
        workers[i] = std::thread(BenchWorker(), i + pIdx * numThreads, timeOut, endCond, workloadType, reqByte);
        // use this when needs to make sure threads from different process write to single file
        // workers[i] = std::thread(BenchWorker(), i + pIdx * numThreads, timeOut, endCond, workloadType, reqByte);
    }
    if(timeOut) {
        sleep(endCond);
        workerRun = false;
    }
    for(int i = 0; i < numThreads; i++) {
        workers[i].join();
    }

}

int main(int argc, char ** argv) {
    std::vector<std::string> tokens;
    binName = std::string(argv[0]);
    for(int i = 1; i < argc; i++) {
        tokens.push_back(std::string(argv[i]));
    }
    benchMain(tokens);
    return 0;
}
