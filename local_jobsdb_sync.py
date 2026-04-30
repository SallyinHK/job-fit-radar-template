from __future__ import annotations

from cloud_runner import load_state, run_one_scan, save_state


def main():
    state = load_state()
    ok = run_one_scan("LOCAL-JOBSDB", "sources_jobsdb_local.yaml", state)
    save_state(state)

    if ok:
        print("Local JobsDB / JobStreet sync completed.")
    else:
        print("Local JobsDB / JobStreet sync failed.")


if __name__ == "__main__":
    main()
