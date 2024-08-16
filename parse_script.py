import json
import re
def parse_beeline_log(file_path):
    query_exec_summary = {}
    task_exec_summary = {}
    detailed_metrics = {}

    current_section = None
    current_metric_group = None

    with open(file_path, 'r') as file:
        for line in file:
            line = line.strip()

            # Detect the start of sections
            if "Query Execution Summary" in line:
                current_section = 'query_exec_summary'
                continue
            elif "Task Execution Summary" in line:
                current_section = 'task_exec_summary'
                continue
            # Detect the start of a new metric group in detailed metrics
            elif line.endswith(':') and line != ("INFO  :") :
                current_section = 'detailed_metrics'
                current_metric_group = line.replace("INFO  :", "").strip().rstrip(':')
                detailed_metrics[current_metric_group] = {}
                continue


            # Parsing based on the current section
            if current_section == 'query_exec_summary':
                # More flexible regex to capture operation names and durations
                match = re.search(r'([\w\s]+?)\s+([\d.]+)s', line)
                if match:
                    operation = match.group(1).strip()
                    duration = float(match.group(2))
                    query_exec_summary[operation] = duration

            elif current_section == 'task_exec_summary':
                # Adjust regex for handling spaces/tabs between columns
                match = re.search(r'([\w\s]+\d*)\s+([\d.,]+)\s+([\d.,]+)\s+([\d.,]+)\s+([\d.,]+)\s+([\d.,]+)', line)
                if match:
                    vertex, duration, cpu_time, gc_time, input_records, output_records = match.groups()
                    task_exec_summary[vertex.strip()] = {
                        "DURATION(ms)": float(duration.replace(',', '')),
                        "CPU_TIME(ms)": int(cpu_time.replace(',', '')),
                        "GC_TIME(ms)": int(gc_time.replace(',', '')),
                        "INPUT_RECORDS": int(input_records.replace(',', '')),
                        "OUTPUT_RECORDS": int(output_records.replace(',', ''))
                    }

            elif current_section == 'detailed_metrics':
                line = line.replace("INFO  :    ", "")
                if " Completed executing command" in line:
                    break
                # Look for key-value pairs within the metric group
                if ':' in line and current_metric_group not in line:
                    key_value = line.split(':', 1)
                    if len(key_value) == 2:
                        key, value = key_value
                        key = key.strip()
                        value = value.strip()#.replace(',', '')
                        # Try to convert value to an integer, if possible
                        try:
                            value = int(value)
                        except ValueError:
                            pass  # Keep the value as a string if it can't be converted
                        detailed_metrics[current_metric_group][key] = value


    # Debug output to verify captured data
    print("Query Execution Summary:", query_exec_summary)
    print("Task Execution Summary:", task_exec_summary)
    print("Detailed Metrics:", detailed_metrics)


    combined_res = {"Query Execution Summary": query_exec_summary,
                    "Task Execution Summary": task_exec_summary,
                    "Detailed Metrics": detailed_metrics
                    }
    with open('combined_results.json', 'w') as f:
        json.dump(combined_res, f, indent=4)


# Run the parser with the provided file path
parse_beeline_log(r'C:\Users\Admin\PycharmProjects\pythonProject\beeline_consent_query_stderr.txt')
