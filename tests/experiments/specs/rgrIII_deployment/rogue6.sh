(sleep 10; echo "SEND rogue9 aGVsbG8gZnJvbSByb2d1ZTYK"; sleep 60) | ./target/release/worker_cli --config tests/experiments/specs/rgrIII_deployment/rogue6.toml --work_dir results/rgrIII_deployment --register_worker false --accept_commands true --log_to_terminal true
