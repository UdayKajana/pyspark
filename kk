import subprocess
import sys
import shlex

def run_untar(input_abs_file, local_dir, out_path):
    # Split path safely
    try:
        input_path = input_abs_file.rsplit('/', 1)[0]
        input_file = input_abs_file.rsplit('/', 1)[-1]
    except IndexError:
        sys.exit("Invalid input file path")
    
    print(f"Input path: {input_path}")
    print(f"Input file: {input_file}")
    
    # Construct command arguments as a list instead of shell string
    cmd = [
        "sh",
        "run_untar.sh",
        input_path,
        input_file,
        local_dir,
        out_path
    ]
    
    # Use list of arguments and shell=False
    print(f"Running command: {' '.join(map(shlex.quote, cmd))}")
    try:
        proc = subprocess.Popen(
            cmd,
            shell=False,  # Prevent shell injection
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True  # Use text mode instead of manual decode
        )
        
        out, err = proc.communicate()
        
        print(f"Process returned code = {proc.returncode}")
        
        if proc.returncode != 0:
            print("----ERROR----")
            print(out)
            sys.exit(f"Failed to execute program: {err}")
            
        return out
        
    except subprocess.SubprocessError as e:
        sys.exit(f"Failed to run command: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        sys.exit("Usage: script.py input_file local_dir out_path")
        
    run_untar(sys.argv[1], sys.argv[2], sys.argv[3])
