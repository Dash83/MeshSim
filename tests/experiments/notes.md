##tmembership_sustained
###Running the test
####1-radio experiment
rm -rf /tmp/experiment/* ; mkdir /tmp/experiment; target/debug/master_cli -d /tmp/experiment -w target/debug/worker_cli -t tests/experiments/specs/tmembership_sustained.toml

####2-radio experiment
rm -rf /tmp/experiment/* ; mkdir /tmp/experiment; target/debug/master_cli -d /tmp/experiment -w target/debug/worker_cli -t tests/experiments/specs/tmembership_advanced_sustained.toml

###Queries for results
--assigns all matching lines, one by one, to variable `a`, effectively keeping just the last one. Performs this operation for each file individually and then prints the values of `a` for each file
awk -F":" 'BEGINFILE { a = ""}  /Membership/ { a=$NF $2 } ENDFILE { if (a != "") print FILENAME, a}' /tmp/experiment/log/node*.log

