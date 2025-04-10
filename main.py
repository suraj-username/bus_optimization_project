from data_loader import load_route_stops, load_passenger_data
from distance_matrix import create_distance_matrix
from route_merger import merge_routes

def prepare_merge_inputs(excel_file, selected_routes, filters=None):
    """
    Prepare all inputs needed for the merge_routes function with filtering.
    
    Args:
        excel_file: Path to Excel file
        selected_routes: List of route numbers to include
        filters: Dictionary of university/year filters
    
    Returns:
        Dictionary containing all inputs for merge_routes
    """
    # Load all data
    stops_df = load_route_stops(excel_file, selected_routes)
    passengers_df = load_passenger_data(excel_file, selected_routes, filters)
    
    # Identify college stop
    college_stop = stops_df[stops_df['Location'].str.contains('College', case=False)].iloc[0]['Location']
    
    # Create routes dictionary
    routes = {}
    for route in selected_routes:
        route_stops = stops_df[stops_df['Route'] == route]['Location'].tolist()
        if route_stops:
            routes[f"Route {route}"] = route_stops
    
    # Calculate stop demands (filtered passengers per stop)
    stop_demands = passengers_df.groupby('Boarding Point').size().to_dict()
    
    # Calculate route_stop_demands (filtered passengers per stop per route)
    route_stop_demands = {}
    for route_id, stops in routes.items():
        route_num = route_id.split()[-1]
        route_passengers = passengers_df[passengers_df['Route'] == route_num]
        demands = route_passengers.groupby('Boarding Point').size().to_dict()
        route_stop_demands[route_id] = demands
    
    # Create distance matrix
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

if __name__ == '__main__':
    excel_file = 'cleaned_file.xlsx'
    selected_routes = ['19', '21', '22', '23', '24']  # Routes to consider
    
    # Define your filters - example: SSN 1st years and faculty, SNU 2nd years
    filters = {
        'SSN': [1, 'Faculty'],
        'SNU': [1, 2, 3]
    }
    
    inputs = prepare_merge_inputs(excel_file, selected_routes, filters)
    
    # Run the merge function
    merged_routes = merge_routes(
        routes=inputs['routes'],
        stop_demands=inputs['stop_demands'],
        distance_matrix=inputs['distance_matrix'],
        college_stop=inputs['college_stop'],
        route_stop_demands=inputs['route_stop_demands']
    )
    
    print("Merged routes:")
    for route_id, stops in merged_routes.items():
        print(f"{route_id}: {stops}")