from data_loader import load_route_stops, load_passenger_data
from distance_matrix import create_distance_matrix
from route_merger import merge_routes
import json

def prepare_merge_inputs(excel_file, selected_routes, filters=None):
    """Prepare all inputs needed for the merge_routes function with filtering."""
    stops_df = load_route_stops(excel_file, selected_routes)
    passengers_df = load_passenger_data(excel_file, selected_routes, filters)
    
    college_stop = stops_df[stops_df['Location'].str.contains('College', case=False)].iloc[0]['Location']
    
    routes = {}
    for route in selected_routes:
        route_stops = stops_df[stops_df['Route'] == route]['Location'].tolist()
        if route_stops:
            routes[f"Route {route}"] = route_stops
    
    stop_demands = passengers_df.groupby('Boarding Point').size().to_dict()
    
    route_stop_demands = {}
    for route_id, stops in routes.items():
        route_num = route_id.split()[-1]
        route_passengers = passengers_df[passengers_df['Route'] == route_num]
        demands = route_passengers.groupby('Boarding Point').size().to_dict()
        route_stop_demands[route_id] = demands
    
    print("Creating distance matrix...")
    distance_matrix = create_distance_matrix(stops_df, college_stop)
    print("Distance matrix created.")
    
    return {
        'routes': routes,
        'stop_demands': stop_demands,
        'distance_matrix': distance_matrix,
        'college_stop': college_stop,
        'route_stop_demands': route_stop_demands
    }

def print_merge_summary(log):
    """Print a clean, well-formatted summary of the merge operations"""
    print("\n=== MERGE OPERATION SUMMARY ===")
    print(f"\nTimestamp: {log['timestamp']}")
    
    # Initial Routes
    print("\nINITIAL ROUTES:")
    for route, data in sorted(log['initial_routes'].items()):
        print(f"\n{route}:")
        print(f"  Total Demand: {data['total_demand']}")
        print("  Stops:")
        for stop, demand in data['stop_demands'].items():
            print(f"    - {stop} (demand: {demand})")
    
    # Removed Routes
    if log['removed_routes']:
        print("\nREMOVED ROUTES:")
        for removal in log['removed_routes']:
            print(f"\n- {removal['route_id']} was merged into:")
            for assignment in removal['stops_assigned']:
                print(f"  • {assignment['stop']} → {assignment['to_route']} "
                      f"(pos: {assignment['position']}, demand: {assignment['demand']})")
    
    # Final Routes
    print("\nFINAL MERGED ROUTES:")
    for route, data in sorted(log['final_routes'].items()):
        print(f"\n{route}:")
        print(f"  Total Demand: {data['total_demand']}")
        print("  Stop Sequence:")
        for i, stop in enumerate(data['stops'], 1):
            # Get the demand from route_stop_demands if available
            print(f"  {i}. {stop}")
    
    # Statistics
    print("\nSTATISTICS:")
    print(f"Initial route count: {len(log['initial_routes'])}")
    print(f"Routes removed: {len(log['removed_routes'])}")
    print(f"Final route count: {len(log['final_routes'])}")
    print(f"Total merge operations: {len(log['merge_operations'])}")

if __name__ == '__main__':
    excel_file = 'cleaned_file.xlsx'
    selected_routes = ['19', '21', '22', '23', '24']
    
    filters = {
        'SSN': [1, 'Faculty'],
        'SNU': [2]
    }
    
    inputs = prepare_merge_inputs(excel_file, selected_routes, filters)
    
    merged_routes, merge_log = merge_routes(
        routes=inputs['routes'],
        stop_demands=inputs['stop_demands'],
        distance_matrix=inputs['distance_matrix'],
        college_stop=inputs['college_stop'],
        route_stop_demands=inputs['route_stop_demands']
    )
    
    # Print the enhanced summary
    print_merge_summary(merge_log)
    
    # Also save the full merged routes
    with open('merged_routes.json', 'w') as f:
        json.dump(merged_routes, f, indent=2)
    print("\nFull merged routes saved to 'merged_routes.json'")