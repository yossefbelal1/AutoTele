import sys

def trace_import():
    print("Reading main_api.py...")
    sys.stdout.flush()
    with open('/app/main_api.py', 'r') as f:
        lines = f.readlines()
    
    # Execute first 100 lines
    code_chunk = "".join(lines[:100])
    print("Executing lines 1-100...")
    sys.stdout.flush()
    exec(code_chunk, globals())
    print("Lines 1-100 executed OK!")
    sys.stdout.flush()

    # Execute lines 101-200
    code_chunk2 = "".join(lines[100:200])
    print("Executing lines 101-200...")
    sys.stdout.flush()
    exec(code_chunk2, globals())
    print("Lines 101-200 executed OK!")
    sys.stdout.flush()

trace_import()
