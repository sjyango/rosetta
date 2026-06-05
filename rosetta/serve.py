"""Headless Rosetta HTTP server for VS Code extension.

Usage:
    python -m rosetta.serve --config ~/.rosetta/config.json --port 19527

Starts the ReportServer with Playground + API endpoints, prints the
port on stdout, then blocks until killed.  No interactive REPL, no
prompt_toolkit dependency.
"""

import argparse
import json
import os
import signal
import sys
import threading


def main():
    from rosetta.paths import CONFIG_FILE, RESULTS_DIR
    parser = argparse.ArgumentParser(description="Rosetta headless HTTP server")
    parser.add_argument("-c", "--config", default=CONFIG_FILE,
                        help="Path to DBMS config JSON")
    parser.add_argument("-p", "--port", type=int, default=0,
                        help="Port (0 = auto)")
    parser.add_argument("-d", "--database", default="cross_dbms_test_db",
                        help="Default database name")
    parser.add_argument("-o", "--output-dir", default=RESULTS_DIR,
                        help="Results output directory")
    args = parser.parse_args()

    # Load config
    from rosetta.config import load_config
    all_configs = load_config(args.config)
    configs = [c for c in all_configs if c.enabled]

    # Set up output directory
    output_dir = os.path.abspath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    # Start server
    from rosetta.interactive import ReportServer
    server = ReportServer(
        directory=output_dir,
        port=args.port,
        configs=configs,
        all_configs=all_configs,
        database=args.database,
    )
    url = server.start()

    # Print port as JSON for the extension to parse
    sys.stdout.write(json.dumps({"port": server.port, "url": url}) + "\n")
    sys.stdout.flush()

    # Block until killed
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    stop.wait()
    server.stop()


if __name__ == "__main__":
    main()
