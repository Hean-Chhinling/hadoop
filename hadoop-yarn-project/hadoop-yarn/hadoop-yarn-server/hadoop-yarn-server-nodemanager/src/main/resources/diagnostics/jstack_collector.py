import subprocess
import sys


def get_java_pid(app_id):

    # process_results= '''
    #     root       413  1.7  2.0 8355580 512972 ?      Sl   11:21   2:56 /usr/lib/jvm/java-8-openjdk//bin/java -Dproc_nodemanager -Djava.net.preferIPv4Stack=true -Dhadoop.log.dir=/opt/hadoop/logs -Dhadoop.log.file=NODEMANAGER.log -Dyarn.log.dir=/opt/hadoop/logs -Dyarn.log.file=NODEMANAGER.log -Dyarn.home.dir=/opt/hadoop -Dyarn.root.logger=INFO,DRFA -Dhadoop.home.dir=/opt/hadoop -Dhadoop.id.str=root -Dhadoop.root.logger=INFO,DRFA -Dhadoop.policy.file=hadoop-policy.xml -Dhadoop.security.logger=INFO,NullAppender -XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.math=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.zip=ALL-UNNAMED --add-opens=java.base/sun.security.util=ALL-UNNAMED --add-opens=java.base/sun.security.x509=ALL-UNNAMED --enable-native-access=ALL-UNNAMED org.apache.hadoop.yarn.server.nodemanager.NodeManager
    #     root     41611  4.1  1.9 2414568 470660 ?      Sl   14:08   0:16 /usr/lib/jvm/java-8-openjdk//bin/java -Xmx750m org.apache.hadoop.yarn.applications.distributedshell.ApplicationMaster --container_type GUARANTEED --container_memory 750 --container_vcores 1 --num_containers 500 --priority 0 --appname DistributedShell --homedir hdfs://namenode:9000/user/root
    #     root     44821  0.0  0.0   2608  1484 pts/0    S+   14:14   0:00 grep jvm/java
    # '''
    process_results = run_command("ps aux | grep jvm/java | grep -vE /bin/bash")
    pids = []
    for result in process_results.strip().splitlines():
        pid = result.split()[1]
        pids.append(pid)

    return pids


def execute_jstack(pids):
    all_jstacks = []
    for pid in pids:
        jstack_output = run_command("jstack", pid)
        all_jstacks.append("--- JStack for PID: {} ---\n{}".format(pid, jstack_output))
    return "\n".join(all_jstacks)


def run_command(*argv):
    try:
        cmd = " ".join(arg for arg in argv)
        print("Running command with arguments:", cmd)
        response = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, check=True)
        response_str = response.stdout.decode('utf-8')
    except subprocess.CalledProcessError as e:
        response_str = "Command failed with error: {}".format(str(e))
        sys.stderr.write("Unable to run command: ", response_str)
        print("Command failed with error: {}".format(str(e)))
    except Exception as e:
        response_str = "Exception occurred: {}".format(str(e))
        sys.stderr.write("Exception occurred: ", response_str)
        print("Exception occurred: {}".format(str(e)))

    return response_str


def main():
    # app_id = sys.argv[1]
    app_id = "application_1748517687882_0013"

    pids = get_java_pid(app_id)
    if not pids:
        sys.stdout.write("No active process id in this NodeManager.")
        sys.exit(0)

    jstacks = execute_jstack(pids[:-1])  # exclude the grep command process
    sys.stdout.write(jstacks)  # The Initiated java processBuilder will read this stdoud

if __name__ == "__main__":
    main()

