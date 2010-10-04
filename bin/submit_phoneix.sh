#!/bin/zsh

#PBS -N phoenix_job
#PBS -l nodes=1:ppn=8
#PBS -V

###############################################################################
#                                                                             #
# submit_phoenix.sh                                                           #
# Adam Wolfe Gordon, June 2010                                                #
#                                                                             #
# Usage: ./submit_phoenix.sh                                                  #
#                                                                             #
# Environment variables: APPNAME, NWORKERS, ARGS                              #
#                                                                             #
# Starts phoenix in virtual machines, runs the specified application with the #
# specified number workers and the given arguments, then shuts down the VMs.  #
#                                                                             #
# Useful for batch scheduled submission of VM-based phoenix jobs when         #
# benchmarking on a shared system.                                            #
#                                                                             #
# Relies on some zsh-isms, so probably don't run it with another shell.       #
#                                                                             #
###############################################################################

# Set these to where phoenix lives on the VMs, and where your VMs live.
# Your VMs must be started by a script called run in $VM_HOME.
PHOENIX_HOME=/home/awolfe/phoenix-2.0.0
VM_HOME=/local/data/awolfe/hadoop_stuff/ubuntu_vms

if [ -z "$APPNAME" ]; then echo "Must specify environment variable APPNAME"; exit 1; fi;
if [ -z "$ARGS" ]; then echo "Must specify environment variable ARGS"; exit 1; fi;
if [ -z "$NWORKERS" ]; then echo "Must specify number of workers"; exit 1; fi;

# If we were submitted with qsub, then go into our work directory.
if [[ -n "$PBS_O_WORKDIR" ]]; then
    cd $PBS_O_WORKDIR;
fi;

rm -f /dev/shm/awolfe;
echo -n "Flushing memory ... ";
bin/flushmem;
echo "Done";

# Check for the hosts file
if [[ ! -e "hosts" ]]; then echo "No hosts file found!"; exit 1; fi;

# Lists of master and workers
MASTER="";
WORKERS=();

# Start the virtual machines
echo -n "Starting up VMs";
$VM_HOME/run;
sleep 5;

# Wait for them to come up
for host in $(cat hosts); do
	WORKERS+=($host);
	MASTER=$host;
	while true; do
		nc -w0 $host 22 2>&1 >/dev/null;
		if [ $? = 0 ]; then break; fi;
	done;
done;

if [ -z "$MASTER" ]; then echo "No master!"; exit 1; fi;
if [[ $#WORKERS < $NWORKERS ]]; then echo "Not enough workers!"; exit 1; fi;

# Sleep a bit more, to make sure the machines get into a nice steady state.
sleep 30;
echo "Done";

# Recompile, to be safe
echo -n "Recompiling Phoenix ... ";
PIDS=();
for host in $(cat hosts); do
	ssh -T $host "cd $PHOENIX_HOME; make clean 2>&1; make 2>&1" 2>&1 > /dev/null &;
	PIDS+=($!);
done;
wait $PIDS;
echo "Done";

# Now we can run our job
echo -n "Running job ... ";
n=0;

for wk in $WORKERS; do
	ssh -T $wk "cd $PHOENIX_HOME; tests/$APPNAME/$APPNAME worker $NWORKERS $ARGS 2>&1" 2>&1 > out-$wk.$PBS_JOBID.txt &;
	n=$(($n + 1));
	if [[ $n -eq $NWORKERS ]]; then break; fi;
done;

sleep 1;

# Run the master
ssh -T $MASTER "cd $PHOENIX_HOME; /usr/bin/time -f '%C -- %U user %S system %P cpu %e total' tests/$APPNAME/$APPNAME master $NWORKERS $ARGS 2>&1" 2>&1 > out-master.$PBS_JOBID.txt;

echo "Done";

# Shut down the VMs - this requires passwordless ssh and passwordless sudo for /sbin/halt
echo -n "Shutting down VMs ... ";
for host in $(cat hosts); do
	ssh -T $host "sudo /sbin/halt 2>&1" 2>&1 >/dev/null;
done;
echo "Done"
