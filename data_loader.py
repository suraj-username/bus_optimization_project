import pandas as pd

def load_route_stops(excel_file, selected_routes):
    """
    Load stop locations for selected routes.
    
    Args:
        excel_file: Path to Excel file
        selected_routes: List of route numbers to include (e.g., ['1', '2', '3'])
    
    Returns:
        DataFrame with all stops from selected routes
    """
    all_stops = []
    
    for route in selected_routes:
        # Handle lowercase a/b in sheet names
        sheet_name = str(route)
        try:
            stops = pd.read_excel(excel_file, sheet_name=sheet_name)
        except:
            # Try with lowercase a/b if not found
            try:
                sheet_name = str(route).upper()
                stops = pd.read_excel(excel_file, sheet_name=sheet_name)
            except:
                print(f"Warning: Could not find sheet for route {route}")
                continue
        
        stops['Route'] = route
        all_stops.append(stops)
    
    return pd.concat(all_stops).drop_duplicates()

def filter_passengers(passengers_df, filters):
    """
    Filter passengers based on university and year conditions.
    
    Args:
        passengers_df: DataFrame containing passenger data
        filters: Dictionary with university as keys and lists of years as values.
                 Use 'Faculty' for faculty members.
                 Example: {'SSN': [1, 'Faculty'], 'SNU': [2]}
    
    Returns:
        Filtered DataFrame
    """
    filtered = []
    
    for university, years in filters.items():
        for year in years:
            if year == 'Faculty':
                # Filter faculty members
                uni_faculty = passengers_df[
                    (passengers_df['University'] == university) &
                    (passengers_df['Passenger'] == 'Faculty')
                ]
                filtered.append(uni_faculty)
            else:
                # Filter students by year
                uni_students = passengers_df[
                    (passengers_df['University'] == university) &
                    (passengers_df['Passenger'] == 'Student') &
                    (passengers_df['Year'] == year)
                ]
                filtered.append(uni_students)
    
    if filtered:
        return pd.concat(filtered)
    return pd.DataFrame()

def load_passenger_data(excel_file, selected_routes, filters=None):
    """
    Load and filter passenger data for selected routes.
    
    Args:
        excel_file: Path to Excel file
        selected_routes: List of route numbers to include
        filters: Dictionary of university/year filters (see filter_passengers)
    
    Returns:
        DataFrame with filtered passenger data
    """
    all_passengers = []
    
    for route in selected_routes:
        # Handle uppercase A/B in sheet names
        sheet_name = f"R{route}"
        try:
            passengers = pd.read_excel(excel_file, sheet_name=sheet_name)
        except:
            # Try with uppercase A/B if not found
            try:
                sheet_name = f"R{route.upper()}"
                passengers = pd.read_excel(excel_file, sheet_name=sheet_name)
            except:
                print(f"Warning: Could not find passenger sheet for route {route}")
                continue
        
        passengers['Route'] = route
        all_passengers.append(passengers)
    
    all_passengers = pd.concat(all_passengers)
    
    if filters:
        return filter_passengers(all_passengers, filters)
    return all_passengers