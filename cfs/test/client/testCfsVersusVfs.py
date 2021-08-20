#!/usr/bin/python3
import os
import re
import string
import sys
import subprocess
import time
import traceback
from queue import Queue
from queue import Empty
import random
from threading import Thread

macro_var_regex = re.compile("%(?P<var>[-0-9]+)%")
macro_rand_uint_regex = re.compile("%RANDUINT\((?P<length>[-0-9A-Z()]+)\)%")
macro_rand_string_regex = re.compile("%RANDSTRING\((?P<length>[-0-9A-Z()]+)\)%")

return_regex = re.compile("return +(?P<retval>[-0-9]+).*")
cfs_not_empty_regex = re.compile("readdir result -- ino:(?!1 )[0-9]+ name:.*")


def queue_output(output, q):
    while True:
        line = output.readline()
        q.put(line.strip())


def get_random_string(length):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))


def get_random_uint(length_in_bits):
    return random.randrange(0, pow(2, length_in_bits))


class ProcessManager:
    def __init__(self, test_bin, test_output_directory, test_set, fsp_shm_id):
        self.data = {
            "instance": {},
            "fdtable": {},  # {vfs: {line: return value}, cfs: {line: return value}}
            "output_queue": {},
            "output_worker_thread": {},
            "line_no": 1
        }
        if "vfs" in test_set:
            self.data["instance"]['vfs'] = subprocess.Popen([test_bin['vfs'], test_output_directory['vfs']],
                                                            stdin=subprocess.PIPE,
                                                            stdout=subprocess.PIPE,
                                                            bufsize=1,
                                                            encoding="utf-8"
                                                            )
        if "cfs" in test_set:
            self.data["instance"]['cfs'] = subprocess.Popen([test_bin['cfs'], repr(fsp_shm_id)],
                                                            stdin=subprocess.PIPE,
                                                            stdout=subprocess.PIPE,
                                                            bufsize=1,
                                                            encoding="utf-8"
                                                            )
        for key in self.data["instance"].keys():
            instance = self.data["instance"][key]
            q = Queue()
            t = Thread(target=queue_output, args=(instance.stdout, q))
            t.daemon = True
            t.start()
            self.data["output_queue"][key] = q
            self.data["output_worker_thread"][key] = t
            time.sleep(0.1)
            while True:
                try:
                    line = q.get_nowait()
                except Empty:
                    break
                else:
                    print("%s -> %s" % (key, line))
            self.data["fdtable"][key] = {}
            self.data["fdtable"][key] = {}

    def check_live(self):
        # Verify that the clients are still alive
        for key in self.data["instance"].keys():
            instance = self.data["instance"][key]
            ret = instance.poll()
            if ret is not None:
                print("%s has exited unexpectedly with status %i" % (key, ret))
                return False
        return True

    def terminus(self):
        for key in self.data["instance"].keys():
            instance = self.data["instance"][key]
            instance.stdin.close()

    def run_command(self, command):
        line_no = self.data["line_no"]
        self.data["line_no"] = line_no + 1

        # Prepare randomness
        rand_uint = 0
        rand_string = ""

        print("====== Line %d ======" % line_no)
        for key in self.data["instance"].keys():
            instance = self.data["instance"][key]
            output = self.data["output_queue"][key]
            input = instance.stdin

            # Tokenize and replace macro in input command
            command_token = command.strip().split(" ")
            for index in range(0, len(command_token)):
                token = command_token[index]
                # Check existence of variable
                matched = macro_var_regex.match(token)
                if matched:
                    # Regular expression lexer only returns string,
                    # so we require explicit casting here.
                    param = int(matched.groupdict()["var"])
                    command_token[index] = self.data["fdtable"][key][param]
                    continue
                # Check if we need to generate a random string
                matched = macro_rand_string_regex.match(token)
                if matched:
                    param = int(matched.groupdict()["length"])
                    if not param:
                        print("Cannot generate a 0-character string")
                        sys.exit(1)
                    if not rand_string:
                        rand_string = get_random_string(param)
                    command_token[index] = rand_string
                    continue
                # Check if we need to generate a random uint
                matched = macro_rand_uint_regex.match(token)
                if matched:
                    param = int(matched.groupdict()["length"])
                    if not rand_uint:
                        rand_uint = get_random_uint(param)
                    command_token[index] = repr(rand_uint)

            real_command = " ".join(command_token)
            print("%s <- %s" % (key, real_command.strip()))
            print(real_command.strip(), file=input, flush=True)
            time.sleep(0.1)
            try:
                while True:
                    line = output.get(timeout=0.4)
                    if not self.check_live():
                        sys.exit(1)
                    print("%s -> %s" % (key, line), flush=True)
                    matched = return_regex.match(line)
                    # Be aware that line_no here is an integer
                    if matched:
                        self.data["fdtable"][key][line_no] = matched.groupdict()["retval"]
            except Empty:
                pass


def sanity_check(test_bin, test_output_directory, test_set, fsp_shm):
    # Check output directories
    # If we are testing on CFS only, test_output_directory isn't really used
    if "vfs" in test_set:
        for key in test_set:
            check_dir = test_output_directory[key]
            if not os.path.isdir(check_dir):
                print("%s is not a directory. Cannot continue." % check_dir)
                return False
            if os.listdir(check_dir):
                print("%s is not empty. Cannot continue." % check_dir)
                return False
    # Check CFS content
    if "cfs" in test_set:
        cfs = subprocess.Popen([test_bin['cfs'], repr(fsp_shm)],
                               stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE,
                               bufsize=1,
                               encoding="utf-8"
                               )
        (out, err) = cfs.communicate(input="lsdir .\n")
        for line in out.split("\n"):
            matched = cfs_not_empty_regex.match(line)
            if matched:
                print("CFS is not empty. Cannot continue.")
                return False
    return True


def usage_and_exit(e):
    print("Usage: testCfsVersysVfs.py -c <fsp shm> -v -i <batch command file>")
    print("    -c <fsp shm>")
    print("            If specified, the testing sequence will run on CFS with this shm id.")
    print("    -v")
    print("            If specified, the testing sequence will run on Linux VFS.")
    print("    -e")
    print("            Allow existing files in cfs / vfs paths")
    print("    If both -c and -v are specified, resulting FS structure will be diffed.")
    print("Each line in a batch file should be a single command, where %KEYWORD% expands:")
    print("    %N% where N is a positive integer")
    print("        Return value of N-th line. Will trigger a KeyError if it refers to a")
    print("        line not encountered yet")
    print("    %RANDUINT(N)%")
    print("        Return a random unsigned integer with at most N bits")
    print("    %RANDSTRING(N)%")
    print("        Return a random string with length N")
    sys.exit(e)


def main():
    tests_prefix_dir = '@CMAKE_CURRENT_BINARY_DIR@/'
    test_bin = {'cfs': tests_prefix_dir + 'testAppCli', 'vfs': tests_prefix_dir + 'testAppCliVfs'}
    test_set = []
    fsp_shm = -1
    input_file = ""
    skip_sanity_check = False

    arg_index = 1
    while arg_index < len(sys.argv):
        current_arg = sys.argv[arg_index]
        if current_arg == "-c":
            test_set.append("cfs")
            if arg_index + 1 >= len(sys.argv):
                usage_and_exit(1)
            arg_index += 1
            try:
                fsp_shm = int(sys.argv[arg_index])
            except ValueError as e:
                print(traceback.format_exc())
                usage_and_exit(1)
        elif current_arg == "-v":
            test_set.append("vfs")
        elif current_arg == "-i":
            if arg_index + 1 >= len(sys.argv):
                usage_and_exit(1)
            arg_index += 1
            input_file = sys.argv[arg_index]
        elif current_arg == "-e":
            skip_sanity_check = True
        arg_index += 1

    if not test_set:
        print("Neither VFS or CFS is specified. Nothing to do.")
        usage_and_exit(0)

    if not input_file:
        print("No input testing sequence specified. Nothing to do.")
        usage_and_exit(0)

    test_output_directory = {'vfs': "/tmp/vfs",
                             'cfs': "/tmp/cfs"}

    if not sanity_check(test_bin, test_output_directory, test_set, fsp_shm):
        if not skip_sanity_check:
            print("Sanity check failed")
            sys.exit(1)
        else:
            print("Warning: Sanity check skipped")
    try:
        batch_fd = open(input_file, "r")
    except IOError as e:
        print("Cannot open batch file")
        sys.exit(1)
    mgr = ProcessManager(test_bin, test_output_directory, test_set, fsp_shm)

    for line in batch_fd:
        mgr.run_command(line)

    mgr.terminus()

    if "vfs" in test_set and "cfs" in test_set:
        # Now dump CFS content out to test_output_directory
        print("===== Dumping CFS =====")
        completed = subprocess.run(["@CMAKE_CURRENT_BINARY_DIR@/testDumpToVfs", repr(fsp_shm), test_output_directory['cfs']])
        if completed.returncode != 0:
            print("===== Dumper returned %d indicated error =====" % completed.returncode)
            sys.exit(completed.returncode)

        # Now do diff
        print("===== Diff start =====")
        completed = subprocess.run(["diff", "-aur", test_output_directory['cfs'], test_output_directory['vfs']])
        print("===== Diff returned %d =====" % completed.returncode)
        sys.exit(completed.returncode)


if __name__ == '__main__':
    main()
