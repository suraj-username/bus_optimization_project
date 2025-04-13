from copy import deepcopy
from datetime import datetime
import json
import pandas as pd
from itertools import combinations

MAX_CAPACITY = 60
DISTANCE_THRESHOLD = 3.0  # in km or whatever unit
DEMAND_IGNORE_THRESHOLD = 2
MAX_IGNORED_DEMAND = 5  # Renamed to better reflect purpose
MIN_CLOSER_THRESHOLD = 0.5

class MergeLogger:
    def __init__(self):
        self.log = {
            'timestamp': datetime.now().isoformat(),
            'initial_routes': {},
            'removed_routes': [],
            'merge_operations': [],
            'final_routes': {},
            'demand_changes': []
        }
    
    def log_initial_state(self, routes, route_demands, route_stop_demands):
        """Log initial state using route_stop_demands to avoid double-counting"""
        self.log['initial_routes'] = {
            route_id: {
                'stops': stops,
                'total_demand': sum(route_stop_demands[route_id].values()),
                'stop_demands': route_stop_demands[route_id]
            }
            for route_id, stops in routes.items()
        }
    
    def log_route_removal(self, route_id, stops_assigned):
        self.log['removed_routes'].append({
            'route_id': route_id,
            'stops_assigned': stops_assigned
        })
    
    def log_merge_operation(self, from_route, to_route, stop, demand, insert_pos):
        self.log['merge_operations'].append({
            'from_route': from_route,
            'to_route': to_route,
            'stop': stop,
            'demand': demand,
            'insert_position': insert_pos,
            'timestamp': datetime.now().isoformat()
        })
    
    def log_final_state(self, routes, route_demands):
        self.log['final_routes'] = {
            route_id: {
                'stops': stops,
                'total_demand': route_demands[route_id]
            }
            for route_id, stops in routes.items()
        }
    
    def save_log(self, filename='route_merge_log.json'):
        with open(filename, 'w') as f:
            json.dump(self.log, f, indent=2)
        return self.log

def load_original_route_order(cleaned_file_path='cleaned_file.xlsx'):
    """Load original route order from Excel file"""
    original_orders = {}
    xls = pd.ExcelFile(cleaned_file_path)
    
    for sheet_name in xls.sheet_names:
        try:
            # Try to convert sheet name to integer (for route numbers)
            route_id = str(int(float(sheet_name)))
        except ValueError:
            # If not a number, keep as is (for routes like '4a')
            route_id = sheet_name
            
        df = pd.read_excel(xls, sheet_name=sheet_name)
        if 'Location' in df.columns:
            original_orders[route_id] = list(df['Location'].dropna().values)
    
    return original_orders

def verify_and_correct_order(route_id, stops, original_orders):
    """Verify and correct stop order based on original route data"""
    if route_id not in original_orders:
        return stops  # Can't verify, return as is
    
    original_stops = original_orders[route_id]
    present_stops = [stop for stop in original_stops if stop in stops]
    
    # Reorder stops based on original order, maintaining only those present
    ordered_stops = []
    for stop in original_stops:
        if stop in stops:
            ordered_stops.append(stop)
    
    return ordered_stops

def try_merge_route(remove_route_id, alive_routes, route_demands, route_stop_demands, 
                   distance_matrix, college_stop, faculty_stops, original_orders, logger):
    """Try to merge a specific route into others, return success and new state"""
    candidate_route = alive_routes[remove_route_id]
    stops_assigned = []
    
    # Create temporary copies for this attempt
    temp_routes = deepcopy(alive_routes)
    temp_route_demands = deepcopy(route_demands)
    temp_route_stop_demands = deepcopy(route_stop_demands)
    
    all_stops_assigned = True
    ignored_demand_sum = 0
    
    for stop in candidate_route:
        stop_route_demand = route_stop_demands[remove_route_id].get(stop, 0)
        
        # Skip low-demand non-faculty stops
        if stop_route_demand <= DEMAND_IGNORE_THRESHOLD and stop not in faculty_stops:
            ignored_demand_sum += stop_route_demand
            continue
        
        best_increase = float('inf')
        best_route_id = None
        best_insert_pos = None
        stop_to_college_dist = distance_matrix[stop][college_stop]
        
        for alive_route_id, alive_stops in temp_routes.items():
            if alive_route_id == remove_route_id:
                continue
            
            if temp_route_demands[alive_route_id] + stop_route_demand > MAX_CAPACITY:
                continue
            
            # Get original order of alive route to maintain sequence
            original_alive_order = original_orders.get(alive_route_id, alive_stops)
            
            for i in range(len(alive_stops)):
                current_stop = alive_stops[i]
                current_to_college_dist = distance_matrix[current_stop][college_stop]
                
                if stop_to_college_dist + MIN_CLOSER_THRESHOLD >= current_to_college_dist:
                    continue
                
                if i == len(alive_stops) - 1:
                    next_stop = college_stop
                else:
                    next_stop = alive_stops[i+1]
                    next_to_college_dist = distance_matrix[next_stop][college_stop]
                    
                    if not (current_to_college_dist >= stop_to_college_dist >= next_to_college_dist):
                        continue
                
                dist_current_to_next = distance_matrix[current_stop][next_stop]
                dist_current_to_stop = distance_matrix[current_stop][stop]
                dist_stop_to_next = distance_matrix[stop][next_stop]
                increase = dist_current_to_stop + dist_stop_to_next - dist_current_to_next
                
                if increase < DISTANCE_THRESHOLD and increase < best_increase:
                    best_increase = increase
                    best_route_id = alive_route_id
                    best_insert_pos = i + 1
        
        if best_route_id is not None:
            temp_routes[best_route_id].insert(best_insert_pos, stop)
            
            if stop not in temp_route_stop_demands[best_route_id]:
                temp_route_stop_demands[best_route_id][stop] = 0
            temp_route_stop_demands[best_route_id][stop] += stop_route_demand
            temp_route_demands[best_route_id] += stop_route_demand
            
            logger.log_merge_operation(
                remove_route_id,
                best_route_id,
                stop,
                stop_route_demand,
                best_insert_pos
            )
            
            stops_assigned.append({
                'stop': stop,
                'to_route': best_route_id,
                'demand': stop_route_demand,
                'position': best_insert_pos
            })
        else:
            all_stops_assigned = False
            break
    
    if ignored_demand_sum > MAX_IGNORED_DEMAND:
        all_stops_assigned = False
    
    if all_stops_assigned:
        del temp_routes[remove_route_id]
        del temp_route_demands[remove_route_id]
        del temp_route_stop_demands[remove_route_id]
        
        # Correct stop orders in all affected routes
        for route_id in temp_routes:
            if route_id in original_orders:
                temp_routes[route_id] = verify_and_correct_order(
                    route_id, temp_routes[route_id], original_orders
                )
        
        logger.log_route_removal(remove_route_id, stops_assigned)
        return True, temp_routes, temp_route_demands, temp_route_stop_demands
    
    return False, None, None, None

def merge_routes(routes, stop_demands, distance_matrix, college_stop, route_stop_demands, faculty_stops):
    """
    Merge routes while respecting constraints with proper demand calculation.
    Uses iterative improvement strategy to find better solutions.
    """
    # Load original route orders from Excel
    original_orders = load_original_route_order()
    
    # Save original routes and demands
    original_routes = deepcopy(routes)
    logger = MergeLogger()
    
    # Initialize route_stop_demands if not provided
    if route_stop_demands is None:
        route_stop_demands = {}
        stop_route_count = {}
        
        # Count how many routes each stop appears in
        for route_id, stops in routes.items():
            for stop in stops:
                if stop not in stop_route_count:
                    stop_route_count[stop] = 0
                stop_route_count[stop] += 1
        
        # Distribute demand evenly among routes containing each stop
        for route_id, stops in routes.items():
            route_stop_demands[route_id] = {}
            for stop in stops:
                if stop in stop_demands:
                    route_stop_demands[route_id][stop] = stop_demands[stop] / stop_route_count[stop]
                else:
                    route_stop_demands[route_id][stop] = 0
    else:
        route_stop_demands = deepcopy(route_stop_demands)
    
    # Keep a copy of the original route_stop_demands
    original_route_stop_demands = deepcopy(route_stop_demands)
    
    # Calculate total demand for each route PROPERLY from route_stop_demands
    route_demands = {
        route_id: sum(demands.values())
        for route_id, demands in route_stop_demands.items()
    }
    
    # Log initial state with proper demand calculation
    logger.log_initial_state(original_routes, route_demands, original_route_stop_demands)
    
    # Track best solution found
    best_solution = {
        'routes': deepcopy(routes),
        'route_demands': deepcopy(route_demands),
        'route_stop_demands': deepcopy(route_stop_demands),
        'routes_removed': 0
    }
    
    # Try different removal orders to find better solutions
    route_ids = list(routes.keys())
    max_routes_to_try = len(route_ids)
    
    for k in range(1, max_routes_to_try + 1):
        for routes_to_remove in combinations(route_ids, k):
            current_routes = deepcopy(routes)
            current_route_demands = deepcopy(route_demands)
            current_route_stop_demands = deepcopy(route_stop_demands)
            temp_logger = deepcopy(logger)
            routes_removed = 0
            
            for route_id in routes_to_remove:
                if route_id not in current_routes:
                    continue
                
                success, new_routes, new_demands, new_stop_demands = try_merge_route(
                    route_id, current_routes, current_route_demands, current_route_stop_demands,
                    distance_matrix, college_stop, faculty_stops, original_orders, temp_logger
                )
                
                if success:
                    current_routes = new_routes
                    current_route_demands = new_demands
                    current_route_stop_demands = new_stop_demands
                    routes_removed += 1
            
            # Update best solution if we found a better one
            if routes_removed > best_solution['routes_removed']:
                best_solution = {
                    'routes': current_routes,
                    'route_demands': current_route_demands,
                    'route_stop_demands': current_route_stop_demands,
                    'routes_removed': routes_removed,
                    'logger': temp_logger
                }
    
    # If no routes could be removed, revert to the original routes
    if best_solution['routes_removed'] == 0:
        print("No routes could be merged. Reverting to original routes.")
        final_routes = original_routes
        final_route_stop_demands = original_route_stop_demands
        final_route_demands = {
            route_id: sum(demands.values())
            for route_id, demands in original_route_stop_demands.items()
        }
        logger.log_final_state(final_routes, final_route_demands)
    else:
        final_routes = best_solution['routes']
        final_route_demands = best_solution['route_demands']
        final_route_stop_demands = best_solution['route_stop_demands']
        best_solution['logger'].log_final_state(final_routes, final_route_demands)
        logger = best_solution['logger']
    
    merge_log = logger.save_log()
    return final_routes, merge_log