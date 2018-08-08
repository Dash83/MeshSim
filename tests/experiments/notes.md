##tmembership_sustained
###Running the test
rm -rf /tmp/experiment/* ; mkdir /tmp/experiment; target/debug/master_cli -d /tmp/experiment -w target/debug/worker_cli -t tests/experiments/specs/tmembership_sustained.toml

###Queries for results
--outputs all the matching lines in each file
awk -F":" '/Membership/ { print $NF $2 } ' /tmp/experiment/log/node*.log

--assigns all matching lines, one by one, to variable `a`, effectively keeping just the last one. Performs this operation for each file individually and then prints the values of `a` for each file
awk -F":" 'BEGINFILE { a = ""}  /Membership/ { a=$NF $2 } ENDFILE { if (a != "") print FILENAME, a}' /tmp/experiment/log/node*.log

