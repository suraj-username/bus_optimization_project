import pandas as pd

# Load the Excel file
file_path = 'routeWiseList.xlsx'
xls = pd.ExcelFile(file_path)

# Create a new writer to save the cleaned file
writer = pd.ExcelWriter('cleaned_file.xlsx', engine='openpyxl')

# Store logs
removed_rows_log = []

# Create a lowercase-to-original-sheet-name map
sheet_name_map = {sheet.lower(): sheet for sheet in xls.sheet_names}

# First, copy non-R sheets as-is
for sheet in xls.sheet_names:
    if not sheet.startswith('R'):
        df = pd.read_excel(file_path, sheet_name=sheet)
        df.to_excel(writer, sheet_name=sheet, index=False)

# Now process R1, R2, ...
for sheet in xls.sheet_names:
    if sheet.startswith('R'):
        print(f"Processing {sheet}...")

        route_no = sheet[1:]  # e.g., "R4A" → "4A"
        route_sheet_key = route_no.lower()  # lowercase matching

        # Read route stops
        if route_sheet_key in sheet_name_map:
            route_sheet_name = sheet_name_map[route_sheet_key]
            stops_df = pd.read_excel(file_path, sheet_name=route_sheet_name)
            stop_names = stops_df['Location'].dropna().str.strip().str.lower().unique()
        else:
            print(f"Warning: No matching route sheet for {sheet}")
            continue

        # Read passenger roster
        roster_df = pd.read_excel(file_path, sheet_name=sheet)

        # Normalize the Boarding Point for comparison
        roster_df['Boarding Point'] = roster_df['Boarding Point'].astype(str).str.strip()

        # Create a mask of valid boarding points
        mask_valid = roster_df['Boarding Point'].str.lower().isin(stop_names)

        # Log removed rows
        removed_rows = roster_df[~mask_valid]
        for _, row in removed_rows.iterrows():
            removed_rows_log.append({
                'Sheet': sheet,
                'Passenger Name': row.get('Name', 'Unknown'),
                'Boarding Point': row.get('Boarding Point', 'Unknown')
            })

        # Keep only valid rows
        cleaned_roster_df = roster_df[mask_valid]

        # Save to output
        cleaned_roster_df.to_excel(writer, sheet_name=sheet, index=False)

# Finally, save the file
writer.close()

# Output the log
log_df = pd.DataFrame(removed_rows_log)
log_df.to_excel('removed_rows_log.xlsx', index=False)

print("✅ Cleaning complete. Removed rows saved to 'removed_rows_log.xlsx'.")
