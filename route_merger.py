from copy import deepcopy
from datetime import datetime
import json

MAX_CAPACITY = 60
DISTANCE_THRESHOLD = 3.0  # in km or whatever unit
DEMAND_IGNORE_THRESHOLD = 0
MAX_DEMAND_SUM_FOR_FAR_STOPS = 0

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

def merge_routes(routes, stop_demands, distance_matrix, college_stop, route_stop_demands, faculty_stops):
    """
    Merge routes while respecting constraints with proper demand calculation.
    If no route can be removed, revert to original routes.
    """
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
    
    # Track if any routes were removed during the process
    any_route_removed = False
    alive_routes = deepcopy(routes)
    
    while True:
        route_removed = False
        
        for remove_route_id in list(alive_routes.keys()):
            candidate_route = alive_routes[remove_route_id]
            stops_assigned = []
            
            # Try to assign each stop to best alive route
            stop_assignments = []
            temp_routes = deepcopy(alive_routes)
            temp_route_demands = deepcopy(route_demands)
            temp_route_stop_demands = deepcopy(route_stop_demands)
            
            all_stops_assigned = True
            far_demand_sum = 0
            
            for stop in candidate_route:
                stop_route_demand = route_stop_demands[remove_route_id].get(stop, 0)
                
                if stop_route_demand <= DEMAND_IGNORE_THRESHOLD and stop not in faculty_stops:
                    continue #Ignore low demand stop unless a faculty is boarding that stop
                
                best_increase = float('inf')
                best_route_id = None
                best_insert_pos = None
                stop_to_college_dist = distance_matrix[stop][college_stop]
                
                for alive_route_id, alive_stops in temp_routes.items():
                    if alive_route_id == remove_route_id:
                        continue
                    
                    if temp_route_demands[alive_route_id] + stop_route_demand > MAX_CAPACITY:
                        continue
                    
                    for i in range(len(alive_stops)):
                        current_stop = alive_stops[i]
                        current_to_college_dist = distance_matrix[current_stop][college_stop]
                        
                        if stop_to_college_dist >= current_to_college_dist:
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
                    stop_assignments.append((stop, best_route_id, best_insert_pos))
                    temp_routes[best_route_id].insert(best_insert_pos, stop)
                    
                    if stop not in temp_route_stop_demands[best_route_id]:
                        temp_route_stop_demands[best_route_id][stop] = 0
                    temp_route_stop_demands[best_route_id][stop] += stop_route_demand
                    temp_route_demands[best_route_id] += stop_route_demand
                    
                    # Log each merge operation
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
                    
                    if best_increase > DISTANCE_THRESHOLD / 2:
                        far_demand_sum += stop_route_demand
                else:
                    all_stops_assigned = False
                    break
            
            if all_stops_assigned and far_demand_sum <= MAX_DEMAND_SUM_FOR_FAR_STOPS:
                alive_routes = temp_routes
                route_demands = temp_route_demands
                route_stop_demands = temp_route_stop_demands
                
                # Log route removal
                logger.log_route_removal(remove_route_id, stops_assigned)
                
                del alive_routes[remove_route_id]
                del route_demands[remove_route_id]
                del route_stop_demands[remove_route_id]
                
                route_removed = True
                any_route_removed = True
                break
        
        if not route_removed:
            break
    
    # If no routes could be removed, revert to the original routes
    if not any_route_removed:
        print("No routes could be merged. Reverting to original routes.")
        alive_routes = original_routes
        route_stop_demands = original_route_stop_demands
        route_demands = {
            route_id: sum(demands.values())
            for route_id, demands in original_route_stop_demands.items()
        }
    
    # Log final state with proper demand calculation
    logger.log_final_state(alive_routes, {
        route_id: sum(demands.values())
        for route_id, demands in route_stop_demands.items()
    })
    
    merge_log = logger.save_log()
    return alive_routes, merge_log