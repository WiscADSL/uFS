

/**
 * Ideally, this tool will generate both headers for c++ and python
 * The goal here is to ease the generating and processing of system log
 * In C++ source code (*.hh) which purely has the struct and the label of output
 *order generate c++ real header will gen the output function to stream generate
 *python core will genearet the python class defination
 **/

struct SnapShotLoadStats {
  int wid = 0;  // @index
  uint64_t version = 0;
  uint64_t rel_nano = 0;
  double avgQlen = 0;
  double lastWindowCpuUtilization = 0;
  uint32_t nReqDone = 0;
  uint32_t nAcIno = 0;
  uint64_t totalReadyCnt[2] = {0, 0};             // @array
  uint16_t inoCycleCntArr[10];  // @array
};                                                // @end
