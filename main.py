from data_loader import load_route_stops, load_passenger_data
from distance_matrix import create_distance_matrix
from route_merger import merge_routes
import json
import os
from datetime import datetime

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

    # Build faculty stops set
    faculty_stops = set(
        passengers_df[
            passengers_df['Passenger'].str.strip().str.lower() == 'faculty'
        ]['Boarding Point'].str.strip()
    )
    
    print("Creating distance matrix...")
    distance_matrix = create_distance_matrix(stops_df, college_stop)
    print("Distance matrix created.")
    
    return {
        'routes': routes,
        'stop_demands': stop_demands,
        'distance_matrix': distance_matrix,
        'college_stop': college_stop,
        'route_stop_demands': route_stop_demands,
        'faculty_stops': faculty_stops
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

def save_overall_statistics(all_logs):
    """Save overall statistics from all merge operations"""
    total_initial_routes = sum(len(log['initial_routes']) for log in all_logs)
    total_removed_routes = sum(len(log['removed_routes']) for log in all_logs)
    total_final_routes = sum(len(log['final_routes']) for log in all_logs)
    total_merge_ops = sum(len(log['merge_operations']) for log in all_logs)
    
    print("\n==== OVERALL STATISTICS ====")
    print(f"Total initial routes processed: {total_initial_routes}")
    print(f"Total routes removed: {total_removed_routes}")
    print(f"Total final routes: {total_final_routes}")
    print(f"Total merge operations: {total_merge_ops}")
    print(f"Route reduction: {total_removed_routes} ({(total_removed_routes / total_initial_routes * 100):.2f}%)")

if __name__ == '__main__':
    excel_file = 'cleaned_file.xlsx'
    selected_routes_groups = [
        ['1', '7', '9a', '9b', '18'],
        ['14', '15', '19', '21', '22', '23', '24'],
        ['4', '4a', '29', '32', '36'],
        ['5', '6', '8', '20', '25', '35', '37'],
        ['10', '27', '34', '38', '38a', '38b', '39', '40'],
        ['41', '42', '42a'],
        ['16', '26', '26a', '30', '31', '33'],
        ['3', '11'],
        ['12', '13'],
        ['2'],
        ['28'],
        ['43'],
        ['9'],
        #['17']
    ]
    
    filters = {
        'SSN': [1, 'Faculty'],
        'SNU': [2]
    }
    
    # Create a directory for merged routes if it doesn't exist
    os.makedirs('merged_routes', exist_ok=True)
    
    # Process each group of routes
    all_merged_routes = {}
    all_logs = []
    
    for group_index, selected_routes in enumerate(selected_routes_groups):
        print(f"\n\n======= PROCESSING ROUTE GROUP {group_index + 1}: {selected_routes} =======")
        
        inputs = prepare_merge_inputs(excel_file, selected_routes, filters)
        
        merged_routes, merge_log = merge_routes(
            routes=inputs['routes'],
            stop_demands=inputs['stop_demands'],
            distance_matrix=inputs['distance_matrix'],
            college_stop=inputs['college_stop'],
            route_stop_demands=inputs['route_stop_demands'],
            faculty_stops=inputs['faculty_stops']
        )
        
        # Print the detailed summary for this group
        print_merge_summary(merge_log)
        
        # Save this group's routes to a separate file
        group_filename = f"merged_routes/group_{group_index + 1}_routes.json"
        with open(group_filename, 'w') as f:
            json.dump(merged_routes, f, indent=2)
        print(f"\nMerged routes for group {group_index + 1} saved to '{group_filename}'")
        
        # Add to overall collection
        all_merged_routes.update(merged_routes)
        all_logs.append(merge_log)
    
    # Save all merged routes combined
    # Sort the keys (route names) to maintain alphabetical/numerical order
    sorted_merged_routes = {k: all_merged_routes[k] for k in sorted(all_merged_routes.keys())}
    with open('merged_routes.json', 'w') as f:
        json.dump(sorted_merged_routes, f, indent=2)
    print("\nAll merged routes combined and saved to 'merged_routes.json'")
    
    # Display overall statistics
    save_overall_statistics(all_logs)