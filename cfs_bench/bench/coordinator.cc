#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <pthread.h>

#include <argp.h>
#include <iostream>

#include "benchutils/coordinator.h"
#include "shmipc/shmipc.h"

/*
 * argument handling
 */
static char DEFAULT_SHMFILE[] = "/coordinator";
static size_t DEFAULT_NUMCLIENTS = 1;

static struct argp_option options[] = {
    {"core", 'c', "CORE_MASK", 0, "core to bind to (must be > 0)"},
    {"shmfile", 'f', "FILE_PATH", 0, "path to shm file for coordinator"},
    {"numclients", 'n', "NUM_CLIENTS", 0,
     "number of clients connecting to fsp"},
    {0}};

struct arguments {
  char *shmfile;
  size_t numclients;
  int core;
};

using namespace std;
static error_t parse_opt(int key, char *arg, struct argp_state *state) {
  struct arguments *arguments = (struct arguments *)state->input;

  switch (key) {
    case 'f':
      arguments->shmfile = arg;
      break;
    case 'n':
      sscanf(arg, "%zd", &(arguments->numclients));
      break;
    case 'c':
      sscanf(arg, "%d", &(arguments->core));
      break;
    case ARGP_KEY_ARG:
      if (state->arg_num > 0) argp_usage(state);
      break;
    case ARGP_KEY_END:
      if (state->arg_num != 0) argp_usage(state);
      break;
    default:
      return ARGP_ERR_UNKNOWN;
  }

  return 0;
}

static struct argp argp = {options, parse_opt, NULL, NULL};

void get_arguments(struct arguments *arguments, int argc, char **argv) {
  arguments->shmfile = DEFAULT_SHMFILE;
  arguments->numclients = DEFAULT_NUMCLIENTS;
  arguments->core = 0;
  argp_parse(&argp, argc, argv, 0, 0, arguments);
}

int bind_to_core(int core) {
  if (core <= 0) {
    cerr << "Invalid core to pin to: " << core << endl;
    return -1;
  }

  pthread_t thread = pthread_self();
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core - 1, &cpuset);
  cout << "Pinning to core: " << core << endl;

  int ret = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
  if (ret != 0) {
    cerr << "Failed to set affinity" << endl;
    return ret;
  }

  ret = pthread_getaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
  if (ret != 0) {
    cerr << "Failed to get affinity" << endl;
    return ret;
  }

  if (!CPU_ISSET(core - 1, &cpuset)) {
    cerr << "Failed to pin core" << endl;
    return -1;
  }

  return 0;
}

int main(int argc, char **argv) {
  struct arguments arguments;
  get_arguments(&arguments, argc, argv);
  cout << "SHMFILE IS " << arguments.shmfile << endl;
  cout << "num clients is " << arguments.numclients << endl;
  if (arguments.core > 0) {
    if (bind_to_core(arguments.core) != 0) {
      cerr << "Failed to bind to core: " << arguments.core << endl;
      return 1;
    }
  }

  CoordinatorServer cs(arguments.shmfile, arguments.numclients);
  cout << "waiting till all clients are ready" << endl;
  cs.wait_till_all_clients_ready();
  cout << "all clients ready, telling them to start" << endl;
  cs.notify_all_clients_to_start();
  cout << "waiting for one client to stop" << endl;
  cs.wait_till_one_client_done();
  cout << "telling all clients to stop" << endl;
  cs.notify_all_clients_to_stop();
  return 0;
}
