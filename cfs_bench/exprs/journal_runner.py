import argparse
from pathlib import Path

from bench_journal import Experiment, MAX_WORKERS, MAX_APPS

parser = argparse.ArgumentParser()
parser.add_argument("--journal", required=True, action="append")
parser.add_argument("--no-journal-repo",
        default="/home/arebello/workspace/fsp_repos/no_journal/ApparateFS")
parser.add_argument("--local-journal-repo",
        default="/home/arebello/workspace/fsp_repos/local_journal/ApparateFS")
parser.add_argument("--global-journal-repo",
        default="/home/arebello/workspace/fsp_repos/global_journal/ApparateFS")
args = parser.parse_args()

journal_repos = {
    "none": args.no_journal_repo,
    "local": args.local_journal_repo,
    "global": args.global_journal_repo,
}

for journal in args.journal:
    assert journal in journal_repos, "Valid journal values are 'none', 'local', 'global'"
    assert (Path(journal_repos[journal]).exists())

wf_app_args = {
    "value_size": 64,
    "numop": 200000,
    "sync_numop": 1
}

w_app_args = {
    "value_size": 64,
    "numop": 200000,
    "sync_numop": 0,
}

for nw in range(1, MAX_WORKERS + 1):
    for na in range(1, MAX_APPS + 1):
        if (nw > na):
            # doesn't make sense if the number of workers are more than number of apps
            continue

        for prefix, app_args in [('wf', wf_app_args), ('w', w_app_args)]:
            # run 'w' only for nw = 1
            if prefix == 'w' and nw != 1:
                continue

            for journal in args.journal:
                try:
                    repo = journal_repos[journal]
                    e = Experiment(f'{prefix}_journal_{journal}', nw, na, repo=repo, **app_args)
                    e.run()
                except KeyboardInterrupt:
                    raise
                except:
                    pass
