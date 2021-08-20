#ifndef __log_h

#define __log_h

#define _LOG(level, msg, ...)                                           \
  do {                                                                  \
    fprintf(stdout, "[" level "] %s:%d " msg "\n", __PRETTY_FUNCTION__, \
            __LINE__, ##__VA_ARGS__);                                   \
  } while (0);

#define LOG(msg, ...) _LOG("INFO", msg, ##__VA_ARGS__);
#define LOG_INFO(msg, ...) _LOG("INFO", msg, ##__VA_ARGS__);
#define LOG_EROR(msg, ...) _LOG("ERROR", msg, ##__VA_ARGS__);
#define LOG_WARN(msg, ...) _LOG("WARNING", msg, ##__VA_ARGS__);

#endif
