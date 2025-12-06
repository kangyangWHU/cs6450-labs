#!/usr/bin/env python3
"""
Automated Experiment Runner for OCC vs 2PL Comparison
This script runs experiments with different configurations and aggregates results
"""

import subprocess
import os
import re
import time
import json
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple


class MethodArguments:
    def __init__(self, num_threads_clients: int, read_ratio: float, theta: float, secs: int):
        
        self.num_threads_clients = num_threads_clients
        self.read_ratio = read_ratio
        self.theta = theta
        self.secs = secs

        if self.read_ratio == 0.95:
            self.workload = 'YCSB-B'
        elif self.read_ratio == 0.5:
            self.workload = 'YCSB-A'
        elif self.read_ratio == 0.75:
            self.workload = 'YCSB-C'
        elif self.read_ratio == 0.25:
            self.workload = 'YCSB-D'
        else:
            self.workload = 'XFER'


    def get_args(self) -> dict[str, list[str]]:
        MethodArgs: dict[str, list[str]] = {
            "2PL": [f"-clients {self.num_threads_clients} -workload {self.workload} -theta {self.theta} -secs {self.secs}", ""],
            "OCC-NoCache": [f"-clients {self.num_threads_clients} -workload {self.workload} -theta {self.theta} -occ -cache-strategy no-cache -secs {self.secs}", "-occ"],
            "OCC-DiscardOnAbort": [f"-clients {self.num_threads_clients} -workload {self.workload} -theta {self.theta} -occ -cache-strategy discard-on-abort -secs {self.secs}", "-occ"],
            "OCC-Proactive": [f"-clients {self.num_threads_clients} -workload {self.workload} -theta {self.theta} -occ -cache-strategy proactive-invalidation -secs {self.secs}", "-occ"],
            "OCC-TTLFixed10ms": [f"-clients {self.num_threads_clients} -workload {self.workload} -theta {self.theta} -occ -cache-strategy ttl-reuse -ttl 10ms -secs {self.secs} -fixed-ttl", "-occ"],
            "OCC-TTLDynamic10ms": [f"-clients {self.num_threads_clients} -workload {self.workload} -theta {self.theta} -occ -cache-strategy ttl-reuse -ttl 10ms -secs {self.secs}", "-occ"],
        }

        return MethodArgs
    
class ExperimentRunner:
    def __init__(self, root_dir: str | None = None):
        """Initialize the experiment runner"""
        self.root_dir = root_dir or os.path.dirname(os.path.abspath(__file__))
        self.logs_dir = os.path.join(self.root_dir, 'logs')
        self.results_dir = os.path.join(self.root_dir, 'results')
        os.makedirs(self.results_dir, exist_ok=True)
        
    def parse_client_log(self, log_path: str) -> Dict:
        """Parse a single client log file and extract statistics"""
        stats = {
            'commits': 0,
            'aborts': 0,
            'cache_hits': 0,
            'cache_misses': 0,
        }
        
        if not os.path.exists(log_path):
            return stats
            
        with open(log_path, 'r') as f:
            content = f.read()
        #      
        # Extract commit/abort stats
        commits_match = re.search(r'Commits:\s*(\d+)\s*\(([\d.]+)%\)', content)
        aborts_match = re.search(r'Aborts:\s*(\d+)\s*\(([\d.]+)%\)', content)
        
        if commits_match:
            stats['commits'] = int(commits_match.group(1))
        if aborts_match:
            stats['aborts'] = int(aborts_match.group(1))
            
        # Extract cache stats
        cache_hits_match = re.search(r'Cache Hits:\s*(\d+)\s*\(([\d.]+)%\)', content)
        cache_misses_match = re.search(r'Cache Misses:\s*(\d+)\s*\(([\d.]+)%\)', content)
        
        if cache_hits_match:
            stats['cache_hits'] = int(cache_hits_match.group(1))
        if cache_misses_match:
            stats['cache_misses'] = int(cache_misses_match.group(1))
            
        return stats
    
    def aggregate_client_results(self, log_dir: str) -> Dict:
        """Aggregate results from all client log files in a directory"""
        aggregated = {
            'num_clients': 0,
            'commits': 0,
            'aborts': 0,
            'cache_hits': 0,
            'cache_misses': 0
        }
        # Find all client log files
        client_logs = sorted(Path(log_dir).glob('kvsclient-*.log'))
        
        if not client_logs:
            return aggregated
            
        for log_file in client_logs:
            stats = self.parse_client_log(str(log_file))
            
            # Sum up all the counts
            aggregated['num_clients'] += 1
            aggregated['commits'] += stats['commits']
            aggregated['aborts'] += stats['aborts']
            aggregated['cache_hits'] += stats['cache_hits']
            aggregated['cache_misses'] += stats['cache_misses']
        
        return aggregated
    
    def run_experiment(self,  client_count: int, server_count: int,
                      client_args: str = "",  server_args: str = "",) -> tuple[bool, str]:
        """Run a single experiment with specified configuration"""
        cmd = ['bash', 'run-cluster.sh', str(server_count), str(client_count)]
        
        cmd.append(server_args)
        if client_args:
            cmd.append(client_args)
            
        print(f"\n{'='*80}")
        print(f"Running: {' '.join(cmd)}")
        print(f"{'='*80}")
        
        try:
            result = subprocess.run(
                cmd,
                cwd=self.root_dir,
                capture_output=True,
                text=True,
                timeout=60  # 1 minute timeout
            )
            
            # Print stdout and stderr for debugging
            if result.stdout:
                print(result.stdout)
            if result.stderr:
                print("STDERR:", result.stderr)
            if result.returncode != 0:
                print(f"Command failed with exit code {result.returncode}")
                return False, ""
            
            # Get the latest log directory
            latest_link = os.path.join(self.logs_dir, 'latest')
            if os.path.exists(latest_link):
                log_dir = os.path.realpath(latest_link)
                return True, log_dir
            else:
                print(f"Error: No latest log directory found")
                return False, ""
                
        except subprocess.TimeoutExpired:
            print(f"Error: Experiment timed out after 10 minutes")
            return False, ""
        except Exception as e:
            print(f"Error running experiment: {e}")
            return False, ""
    
    def run_experiments(self, client_count: int, server_count: int, 
                              num_threads_clients: list, contention_levels: list, 
                              read_ratios: list, secs: int) -> None:
        """Run all experiments according to the research plan"""
        return
        # Experiment configurations
        experiments = []
        
        # YCSB-B workload (95% reads, 5% writes) with different contention levels
        # contention_levels = [0.01, 0.25, 0.5, 0.75, 0.99]
        # contention_levels = [0.5]
        # OCC experiments with YCSB-B and different contention
        for theta in contention_levels:
            for read_ratio in read_ratios:
                for num_threads_client in num_threads_clients:
                    all_args = MethodArguments(num_threads_clients=num_threads_client, read_ratio=read_ratio, theta=theta, secs=secs).get_args()
                    experiments.append(all_args)
        
       
        # Store all results
        all_results = []
        
        # Run each experiment
        for i, exp in enumerate(experiments, 1):

            method_res = []
            for j, (key, args) in enumerate(exp.items(), 1):
                print(f"\n\n{'#'*80}")
                print(f"Experiment {(i-1)*len(exp)+j}/{len(experiments)*len(exp)}: {key}: {args[0]}")
                print(f"{'#'*80}")
                
                success, log_dir = self.run_experiment(
                    client_count,
                    server_count,
                    args[0],
                    args[1]
                )
                
                if success and log_dir:
                    # Aggregate results
                    results = self.aggregate_client_results(log_dir)
                    results['method'] = key  # Add method name
                    results['log_dir'] = log_dir
                    
                    # Extract num_threads from client args
                    if '-clients' in args[0]:
                        match = re.search(r'-clients (\d+)', args[0])
                        if match:
                            results['num_threads'] = int(match.group(1))
                    
                    results['server_count'] = server_count

                    # Extract theta from client args
                    if '-theta' in args[0]:
                        match = re.search(r'-theta ([\d.]+)', args[0])
                        if match:
                            results['theta'] = float(match.group(1))
                    
                    # Extract secs from client args
                    if '-secs' in args[0]:
                        match = re.search(r'-secs (\d+)', args[0])
                        if match:
                            results['secs'] = int(match.group(1))
                    else:
                        results['secs'] = 1  # Default to 1 to avoid division by zero
                    
                    # Extract workload from client args
                    if '-workload' in args[0]:
                        match = re.search(r'-workload (\S+)', args[0])
                        if match:
                            results['workload'] = match.group(1)
                            if results['workload'] == 'YCSB-B':
                                results['read_ratio'] = 0.95
                            elif results['workload'] == 'YCSB-A':
                                results['read_ratio'] = 0.5
                            elif results['workload'] == 'YCSB-C':      
                                results['read_ratio'] = 0.75
                            elif results['workload'] == 'YCSB-D':
                                results['read_ratio'] = 0.25
                            else:
                                results['read_ratio'] = 0.0
                    # Check if using OCC
                    if '-occ' in args[0]:
                        results['use_occ'] = True
                    else:
                        results['use_occ'] = False
                    
                    # calculate totals and rates
                    total_txns = results['commits'] + results['aborts']
                    secs = results['secs']
                    results['total_transactions'] = total_txns
                    results['commit_rate'] = (results['commits'] / total_txns * 100) if total_txns > 0 else 0.0
                    results['abort_rate'] = (results['aborts'] / total_txns * 100) if total_txns > 0 else 0.0
                    results['commits_per_sec'] = results['commits'] / secs
                    results['aborts_per_sec'] = results['aborts'] / secs
                    total_cache_accesses = results['cache_hits'] + results['cache_misses']
                    results['total_cache_accesses'] = total_cache_accesses
                    results['cache_hit_rate'] = (results['cache_hits'] / total_cache_accesses * 100) if total_cache_accesses > 0 else 0.0
                    results['cache_miss_rate'] = (results['cache_misses'] / total_cache_accesses * 100) if total_cache_accesses > 0 else 0.0
                    results['cache_hits_per_sec'] = results['cache_hits'] / secs
                    results['cache_misses_per_sec'] = results['cache_misses'] / secs
                    
                    method_res.append(results)
                   
                else:
                    print(f"Experiment failed for method {key}")
                
            # Small delay between experiments
            all_results.append(method_res)

        # Save results
        self.print_summary_table(all_results)
        
        # Plot results
        if len(contention_levels) > 1:
             self.plot_line('contention level', 'commits_per_sec', all_results)
             self.plot_line('contention level', 'abort_rate', all_results)
             self.plot_line('contention level', 'cache_hit_rate', all_results)
             self.plot_line('contention level', 'cache_hits_per_sec', all_results)
        
        if len(read_ratios) > 1:
             self.plot_line('read ratio', 'commits_per_sec', all_results)
             self.plot_line('read ratio', 'abort_rate', all_results)
             self.plot_line('read ratio', 'cache_hit_rate', all_results)
             self.plot_line('read ratio', 'cache_hits_per_sec', all_results)

        # return all_results

        # save results to json
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        with open(os.path.join(self.results_dir, f"results_{timestamp}.json"), 'w') as f:
            json.dump(all_results, f, indent=4)

    # def run_workload_experiments(self, client_count: int, server_count: int, 
    #                           num_threads_clients: int, theta: float, secs: int):
        
        
    def print_summary_table(self, results: List[List[Dict]]):
        """Print a summary table of all results with separate tables for each metric"""
        
        # Organize data by theta and method
        # results is a list of lists, where each inner list contains results for all methods at a given theta
        
        # Extract method names from first result set
        if not results or not results[0]:
            print("No results to display")
            return
        
        methods = list(results[0][0].keys())  # Assuming all methods are present in first theta
        thetas = []
        
        # Build data structure: {theta: {method: metrics}}
        data_by_theta = {}
        for theta_results in results:
            if theta_results and len(theta_results) > 0:
                theta = theta_results[0].get('theta', 0.0)
                thetas.append(theta)
                data_by_theta[theta] = {}
                
                for method_result in theta_results:
                    method = method_result.get('method', 'Unknown')
                    data_by_theta[theta][method] = method_result
        
        if not data_by_theta:
            print("No valid results to display")
            return
        
        # Get all unique methods in the order they were defined
        # method_order = ["2PL", "OCC-NoCache", "OCC-DiscardOnAbort", "OCC-Proactive", "OCC-TTLReuse100", "OCC-TTLReuse10"]
        method_order = ["OCC-NoCache", "OCC-Proactive",  "OCC-TTLFixed10ms", "OCC-TTLDynamic10ms"]
        all_methods = []
        for method in method_order:
            # Check if this method exists in any of the results
            for theta_data in data_by_theta.values():
                if method in theta_data:
                    all_methods.append(method)
                    break
        
        print(f"\n\n{'='*140}")
        print("EXPERIMENT RESULTS SUMMARY")
        print(f"{'='*140}\n")
        
        # Helper function to print a metric table
        def print_metric_table(metric_name, metric_key, format_str="{:.0f}"):
            print(f"\n{metric_name}")
            print("-" * 140)
            
            # Header
            header = f"{'Theta':<10}"
            for method in all_methods:
                header += f"{method:<20}"
            print(header)
            print("-" * 140)
            
            # Rows for each theta
            for theta in sorted(thetas):
                row = f"{theta:<10.2f}"
                for method in all_methods:
                    if method in data_by_theta[theta]:
                        value = data_by_theta[theta][method].get(metric_key, 0)
                        row += format_str.format(value).ljust(20)
                    else:
                        row += "N/A".ljust(20)
                print(row)
            print()
        
        # Print tables for each metric
        # print_metric_table("Commits", "commits", "{:.0f}")
        print_metric_table("Commits/sec", "commits_per_sec", "{:.2f}")
        # print_metric_table("Commit Rate (%)", "commit_rate", "{:.2f}")
        # print_metric_table("Aborts", "aborts", "{:.0f}")
        # print_metric_table("Aborts/sec", "aborts_per_sec", "{:.2f}")
        print_metric_table("Abort Rate (%)", "abort_rate", "{:.2f}")
        # print_metric_table("Cache Hits", "cache_hits", "{:.0f}")
        print_metric_table("Cache Hits/sec", "cache_hits_per_sec", "{:.2f}")
        print_metric_table("Cache Hit Rate (%)", "cache_hit_rate", "{:.2f}")
        # print_metric_table("Cache Misses", "cache_misses", "{:.0f}")
        # print_metric_table("Cache Misses/sec", "cache_misses_per_sec", "{:.2f}")
        # print_metric_table("Cache Miss Rate (%)", "cache_miss_rate", "{:.2f}")
        
        print(f"{'='*140}\n")



    @staticmethod
    def plot_line(xfield: str, metric:str, results: List[List[Dict]]):
        """Plot line graphs for key metrics"""
        if not results:
            print("No results to plot")
            return

        # Flatten results
        flat_results = []
        for sublist in results:
            flat_results.extend(sublist)
            
        if not flat_results:
            print("No results to plot")
            return

        # Map xfield to key
        x_key_map = {
            'contention level': 'theta',
            'read ratio': 'read_ratio',
            'num_threads_clients': 'num_threads',
            'servercount': 'server_count',
            'sec': 'secs'
        }
        
        x_key = x_key_map.get(xfield, xfield)
        
        # Group by method
        methods = {}
        for res in flat_results:
            method = res.get('method', 'Unknown')
            if method not in methods:
                methods[method] = []
            
            if x_key in res and metric in res:
                methods[method].append((res[x_key], res[metric]))
        
        # Plot
        plt.figure(figsize=(6, 4))
        
        COLORS = ["#be443d", "#1a603c", "#da6200", "#00aab5", "C4", "#800080"]
        MARKER = ["P", "^", "v", "o", "*", "X"]
        
        all_x_values = set()
        
        for i, (method, points) in enumerate(methods.items()):
            if not points:
                continue
            # Sort by x
            points.sort(key=lambda p: p[0])
            x_vals = [p[0] for p in points]
            y_vals = [p[1] for p in points]
            
            all_x_values.update(x_vals)
            
            plt.plot(x_vals, y_vals, marker=MARKER[i % len(MARKER)], color=COLORS[i % len(COLORS)], label=method)
            
        # Format labels: remove underscores and capitalize words
        xlabel = xfield.replace('_', ' ').title()
        ylabel = metric.replace('_', ' ').title()

        if ylabel == "Commits Per Sec":
            ylabel = "Commits/sec"
        elif ylabel == "Commit Rate":
            ylabel = "Commit Rate (%)"
        elif ylabel == "Cache Hit Rate":
            ylabel = "Cache Hit Rate (%)"
        elif ylabel == "Cache Hits Per Sec":
            ylabel = "Cache Hits/sec"
        
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.title(f'{ylabel} Vs {xlabel}')
        plt.legend()
        plt.grid(True)
        
        # if xfield == 'read ratio':
        plt.xticks(sorted(list(all_x_values)))

        plt.tight_layout()
        plt.subplots_adjust(
            top=0.99,
            bottom= 0.15,
            left=0.15,
            right=0.99,
            hspace=0.2,
            wspace=0.2
        )
        # Save plot
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"plot_{metric}_vs_{xfield.replace(' ', '_')}_{timestamp}.pdf"
        
        save_dir = 'results'
        if not os.path.exists(save_dir):
            os.makedirs(save_dir, exist_ok=True)
            
        plt.savefig(os.path.join(save_dir, filename))
        print(f"Plot saved to {os.path.join(save_dir, filename)}")
        plt.close()

def main():
    """Main entry point"""
    runner = ExperimentRunner(root_dir=None)
    
    print("="*60)
    print("OCC vs 2PL Experiment Runner")
    print("="*60)
    print("\nThis script will run a comprehensive set of experiments to compare")
    print("OCC with different cache strategies against 2PL baseline.")
    print("\nExperiments include:")
    print("- YCSB-B workload with different contention levels (θ = 0, 0.25, 0.5, 0.75, 0.99)")
    print("- YCSB-A and YCSB-C workloads")
    print("- Multiple cache strategies (discard-on-abort, proactive-invalidation, ttl-reuse, no-cache)")
    print("- 2PL baseline for comparison")
    
    
    # Run all experiments
    runner.run_experiments(client_count=2, server_count=2,num_threads_clients=[10, 20, 30, 40, 50],
                                  contention_levels=[0.99], read_ratios=[0.25], secs=10)

    with open('/mnt/nfs/dsfinal/cs6450-labs/results/clients/results_20251204_111622.json', 'r') as f:
        all_results = json.load(f)
    runner.plot_line('num_threads_clients', 'commits_per_sec', all_results)
    runner.plot_line('num_threads_clients', 'commit_rate', all_results)
    runner.plot_line('num_threads_clients', 'cache_hit_rate', all_results)
    runner.plot_line('num_threads_clients', 'cache_hits_per_sec', all_results)
    print("\nAll experiments completed!")


if __name__ == '__main__':
    main()
