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

            # Detect the start of sections and update current_section and current_metric_group
            current_section, current_metric_group = update_section(line, current_section, current_metric_group,
                                                                   detailed_metrics)

            # Parse based on the current section
            if current_section == 'query_exec_summary':
                parse_query_execution_summary(line, query_exec_summary)
            elif current_section == 'task_exec_summary':
                parse_task_execution_summary(line, task_exec_summary)
            elif current_section == 'detailed_metrics':
                parse_detailed_metrics(line, current_metric_group, detailed_metrics)


    # Combine results and write to file
    combined_res = {
        "Query Execution Summary": query_exec_summary,
        "Task Execution Summary": task_exec_summary,
        "Detailed Metrics": detailed_metrics
    }

    with open('combined_results2.json', 'w') as f:
        json.dump(combined_res, f, indent=4)


# Function to update the section and handle section transitions
def update_section(line, current_section, current_metric_group, detailed_metrics):
    if "Query Execution Summary" in line:
        return 'query_exec_summary', None
    elif "Task Execution Summary" in line:
        return 'task_exec_summary', None
    elif line.endswith(':') and line != "INFO  :":
        current_metric_group = line.replace("INFO  :", "").strip().rstrip(':')
        detailed_metrics[current_metric_group] = {}  # Initialize the metric group
        return 'detailed_metrics', current_metric_group
    elif "Completed executing command" in line:
        return None,None
    return current_section, current_metric_group  # Keep current metric group when no section transition


# Function to parse the Query Execution Summary section
def parse_query_execution_summary(line, query_exec_summary):
    match = re.search(r'([\w\s]+?)\s+([\d.]+)s', line)
    if match:
        operation = match.group(1).strip()
        duration = float(match.group(2))
        query_exec_summary[operation] = duration


# Function to parse the Task Execution Summary section
def parse_task_execution_summary(line, task_exec_summary):
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


# Function to parse the Detailed Metrics section
def parse_detailed_metrics(line, current_metric_group, detailed_metrics):
    line = line.replace("INFO  :    ", "")

    if ':' in line and current_metric_group and current_metric_group not in line:
        key_value = line.split(':', 1)
        if len(key_value) == 2:
            key, value = key_value
            key = key.strip()
            value = value.strip()
            try:
                value = int(value)
            except ValueError:
                pass
            detailed_metrics[current_metric_group][key] = value


# Run the parser with the provided file path
parse_beeline_log(r'C:\Users\Admin\PycharmProjects\pythonProject\beeline_consent_query_stderr.txt')
