import subprocess
import sys
input_abs_file = sys.argv[1]
local_dir= sys.argv[2]
out_path = sys.argv[3]
input_path = input_abs_file.rsplit('/', 1)[0]
print(input_path)
input_file = input_abs_file.rsplit('/', 1)[-1]
print(input_file)
print("command to run: sh run_untar.sh {} {} {} {}".format(input_path,input_file,local_dir,out_path))
# Fixed line: Pass arguments as list instead of shell string
proc = subprocess.Popen(["sh", "run_untar.sh", input_path, input_file, local_dir, out_path],
                       shell=False,  # Prevent shell injection
                       stdout=subprocess.PIPE, 
                       stderr=subprocess.PIPE)
out,err = proc.communicate()
print ("err returned code = %d" % proc.returncode)
if(proc.returncode != 0):
  print("----ERROR----")
  print(out.decode('utf-8'))
  sys.exit(f"failed to execute program :{err.decode('utf-8')}")
