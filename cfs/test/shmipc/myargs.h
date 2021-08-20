#ifndef __myargs_h
#define __myargs_h
static char DEFAULT_SHMFILE[] = "/testshm";
static int DEFAULT_CREATE = 0;
static int DEFAULT_RSIZE = 20;
static int DEFAULT_ROUNDS = 100;
static int DEFAULT_XREQ = 0;

static struct argp_option options[] = {
    {"shmfile", 'f', "FILE", 0, "alternate shm file, defaults to /testshm"},
    {"create", 'c', 0, 0,
     "create and truncate the shm file (as opposed to just using it)"},
    {"rsize", 's', "RING_SIZE", 0, "capactiy of the ring"},
    {"rounds", 'r', "ROUNDS", 0, "number of ping pongs"},
    {"xreq", 'x', 0, 0, "use xrequests"},
    {0}};

struct arguments {
  char *shmfile;
  size_t rsize;
  int create;
  int rounds;
  int xreq;
};

static error_t parse_opt(int key, char *arg, struct argp_state *state) {
  struct arguments *arguments = state->input;

  switch (key) {
    case 'f':
      arguments->shmfile = arg;
      break;
    case 's':
      sscanf(arg, "%zd", &(arguments->rsize));
      break;
    case 'c':
      arguments->create = 1;
      break;
    case 'r':
      sscanf(arg, "%d", &(arguments->rounds));
      break;
    case 'x':
      arguments->xreq = 1;
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
  arguments->create = DEFAULT_CREATE;
  arguments->rsize = DEFAULT_RSIZE;
  arguments->rounds = DEFAULT_ROUNDS;
  arguments->xreq = DEFAULT_XREQ;
  argp_parse(&argp, argc, argv, 0, 0, arguments);
}

#endif
