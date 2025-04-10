from copy import deepcopy

MAX_CAPACITY = 60
DISTANCE_THRESHOLD = 5.0  # in km or whatever unit
DEMAND_IGNORE_THRESHOLD = 3
MAX_DEMAND_SUM_FOR_FAR_STOPS = 5

def merge_routes(routes, stop_demands, distance_matrix, college_stop, route_stop_demands=None):
    """
    Merge routes while respecting constraints.
    
    Args:
        routes: Dictionary mapping route_id to list of stops
        stop_demands: Dictionary mapping each stop to its total demand
        distance_matrix: Nested dictionary with distances between stops
        college_stop: The stop representing the college
        route_stop_demands: Dictionary mapping route_id to a dictionary of {stop: demand} 
                            that specifies how much demand each route handles at each stop
    """
    alive_routes = deepcopy(routes)
    
    # Initialize route_stop_demands if not provided
    if route_stop_demands is None:
        # Default: evenly distribute demand if stops are shared
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
    
    # Calculate total demand for each route
    route_demands = {route_id: sum(route_stop_demands[route_id].get(stop, 0) for stop in stops) 
                     for route_id, stops in alive_routes.items()}
    
    while True:
        route_removed = False
        
        for remove_route_id in list(alive_routes.keys()):
            candidate_route = alive_routes[remove_route_id]
            
            # Try to assign each stop to best alive route
            stop_assignments = []
            temp_routes = deepcopy(alive_routes)
            temp_route_demands = deepcopy(route_demands)
            temp_route_stop_demands = deepcopy(route_stop_demands)
            
            all_stops_assigned = True
            far_demand_sum = 0
            
            for stop in candidate_route:
                # Get the demand this route is handling for this stop
                stop_route_demand = route_stop_demands[remove_route_id].get(stop, 0)
                
                # Skip if demand is negligible
                if stop_route_demand <= DEMAND_IGNORE_THRESHOLD:
                    continue
                
                best_increase = float('inf')
                best_route_id = None
                best_insert_pos = None
                
                # Distance from this stop to college (for college direction check)
                stop_to_college_dist = distance_matrix[stop][college_stop]
                
                for alive_route_id, alive_stops in temp_routes.items():
                    if alive_route_id == remove_route_id:
                        continue
                    
                    # Check if route has capacity for this additional demand
                    if temp_route_demands[alive_route_id] + stop_route_demand > MAX_CAPACITY:
                        continue
                    
                    for i in range(len(alive_stops)):
                        current_stop = alive_stops[i]
                        
                        # College direction check: ensure we're moving closer to college
                        current_to_college_dist = distance_matrix[current_stop][college_stop]
                        
                        # Skip if inserting here would move away from college
                        if stop_to_college_dist >= current_to_college_dist:
                            continue
                        
                        if i == len(alive_stops) - 1:
                            next_stop = college_stop
                        else:
                            next_stop = alive_stops[i+1]
                            next_to_college_dist = distance_matrix[next_stop][college_stop]
                            
                            # Also ensure the stop is between current and next in terms of college proximity
                            if not (current_to_college_dist >= stop_to_college_dist >= next_to_college_dist):
                                continue
                        
                        # Calculate distance increase
                        dist_current_to_next = distance_matrix[current_stop][next_stop]
                        dist_current_to_stop = distance_matrix[current_stop][stop]
                        dist_stop_to_next = distance_matrix[stop][next_stop]
                        
                        increase = dist_current_to_stop + dist_stop_to_next - dist_current_to_next
                        
                        if increase < DISTANCE_THRESHOLD:
                            if increase < best_increase:
                                best_increase = increase
                                best_route_id = alive_route_id
                                best_insert_pos = i + 1
                
                if best_route_id is not None:
                    # Track insertions
                    stop_assignments.append((stop, best_route_id, best_insert_pos))
                    
                    # Update temp routes and demands
                    temp_routes[best_route_id].insert(best_insert_pos, stop)
                    
                    # Transfer the demand from the removed route to the receiving route
                    if stop not in temp_route_stop_demands[best_route_id]:
                        temp_route_stop_demands[best_route_id][stop] = 0
                    temp_route_stop_demands[best_route_id][stop] += stop_route_demand
                    temp_route_demands[best_route_id] += stop_route_demand
                    
                    # Track "far" stops
                    if best_increase > DISTANCE_THRESHOLD / 2:
                        far_demand_sum += stop_route_demand
                else:
                    # Cannot assign this stop
                    all_stops_assigned = False
                    break
            
            # If we can remove this route
            if all_stops_assigned and far_demand_sum <= MAX_DEMAND_SUM_FOR_FAR_STOPS:
                # Apply all assignments
                alive_routes = temp_routes
                route_demands = temp_route_demands
                route_stop_demands = temp_route_stop_demands
                
                # Remove the distributed route
                del alive_routes[remove_route_id]
                del route_demands[remove_route_id]
                del route_stop_demands[remove_route_id]
                
                route_removed = True
                break  # Restart with new state as routes have changed
        
        if not route_removed:
            # No more routes can be removed
            break
    
    return alive_routes

if __name__ == '__main__':
    pass