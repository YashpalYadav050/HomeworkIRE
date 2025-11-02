import json

# Read notebook
with open('main.ipynb', 'r', encoding='utf-8') as f:
    notebook = json.load(f)

# Find the cell with the wait loop (Cell 5, index 4)
target_cell = None
for i, cell in enumerate(notebook['cells']):
    if cell.get('cell_type') == 'code' and i >= 4:
        source = ''.join(cell.get('source', []))
        if 'check_interval = 2' in source and 'Waiting for Elasticsearch' in source:
            target_cell = i
            break

if target_cell is None:
    print("Could not find target cell")
    exit(1)

# Update the source code
cell = notebook['cells'][target_cell]
source_lines = cell['source']

# Find and replace the wait loop section
new_source = []
i = 0
while i < len(source_lines):
    line = source_lines[i]
    
    # Find the section to replace
    if 'check_interval = 2' in line:
        # Replace from this line until the end of the while loop
        new_source.append('            check_interval = 1  # Changed from 2 to 1 second for faster detection\n')
        new_source.append('            last_log_show = 0\n')
        new_source.append('            last_status_print = 0\n')
        new_source.append('            \n')
        new_source.append('            while True:\n')
        new_source.append('                time.sleep(check_interval)\n')
        new_source.append('                waited += check_interval\n')
        new_source.append('                \n')
        new_source.append('                # Show container logs every 8 seconds to see what\'s happening\n')
        new_source.append('                if waited - last_log_show >= 8:\n')
        new_source.append('                    try:\n')
        new_source.append('                        log_result = subprocess.run(\n')
        new_source.append('                            ["docker", "logs", "--tail", "3", ES_CONTAINER_NAME],\n')
        new_source.append('                            capture_output=True,\n')
        new_source.append('                            text=True\n')
        new_source.append('                        )\n')
        new_source.append('                        if log_result.stdout.strip():\n')
        new_source.append('                            lines = log_result.stdout.strip().split(\'\\\\n\')\n')
        new_source.append('                            for line in lines[-3:]:  # Show last 3 log lines\n')
        new_source.append('                                # Only show important lines\n')
        new_source.append('                                if any(keyword in line.lower() for keyword in [\'started\', \'ready\', \'initialized\', \'error\', \'exception\', \'bound\', \'publish\']):\n')
        new_source.append('                                    display_line = line[:100] if len(line) > 100 else line\n')
        new_source.append('                                    print(f"   📋 {display_line}")\n')
        new_source.append('                                    last_log_show = waited\n')
        new_source.append('                    except:\n')
        new_source.append('                        pass\n')
        new_source.append('                \n')
        new_source.append('                # Print status every 10 seconds\n')
        new_source.append('                if waited % 10 == 0 and waited > 0:\n')
        new_source.append('                    print(f"   ⏳ Still initializing... ({waited}s elapsed)")\n')
        new_source.append('                    last_status_print = waited\n')
        new_source.append('                \n')
        new_source.append('                # Check Elasticsearch readiness (check every second)\n')
        new_source.append('                try:\n')
        new_source.append('                    test_es = get_es()\n')
        new_source.append('                    if test_es.ping():\n')
        new_source.append('                        if waited > 5:\n')
        new_source.append('                            print()\n')
        new_source.append('                        print(f"✅ Elasticsearch is ready! (took {waited}s)")\n')
        new_source.append('                        return True\n')
        new_source.append('                except:\n')
        new_source.append('                    pass\n')
        
        # Skip the old wait loop lines
        while i < len(source_lines):
            if 'except:' in source_lines[i] and 'pass' in source_lines[i+1] if i+1 < len(source_lines) else False:
                i += 2
                break
            i += 1
        continue
    
    # Update the message about timing
    if 'This usually takes 15-30 seconds' in line:
        new_source.append('            print(f"   (Normal: 20-45 seconds - Elasticsearch booting JVM, initializing cluster, starting HTTP service)")\n')
        new_source.append('            print(f"   Checking every second and showing logs periodically...\\\\n")\n')
        i += 1
        continue
    
    new_source.append(line)
    i += 1

cell['source'] = new_source

# Write back
with open('main.ipynb', 'w', encoding='utf-8') as f:
    json.dump(notebook, f, indent=1, ensure_ascii=False)

print("✅ Updated notebook! The wait loop now:")
print("   - Checks every 1 second (was 2 seconds)")
print("   - Shows container logs every 8 seconds")
print("   - Provides better feedback about what's happening")

