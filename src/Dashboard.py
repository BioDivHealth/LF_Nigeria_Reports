import pandas as pd
import plotly.express as px
import dash
from dash import dcc, html, Input, Output, State

# --- 1. Data Loading and Cleaning ---
# (Adapted from the previous cleaning script)

# Configuration
file_path = 'data/processed/combined_lassa_data_2021-2025.csv'
numeric_cols_original = [
    'Suspected', 'Confirmed', 'Probable', 'HCW.', 'Deaths..Confirmed.Cases.', 'Year', 'Week'
]
rename_mapping = {
    'States': 'State',  # Fixed: 'States' to 'State' (not 'tates' which was incorrect)
    'Deaths..Confirmed.Cases.': 'Deaths',
    'HCW.': 'HCW_Confirmed'
}

# Function to load and clean data
def load_and_clean_data(file_path):
    """Loads and cleans the Lassa fever data."""
    try:
        df = pd.read_csv(file_path)
        print("File loaded successfully.")
        
        # Debug: Print column names to identify what's available
        print(f"Available columns in CSV: {df.columns.tolist()}")

        # Rename columns
        df.rename(columns=rename_mapping, inplace=True)
        # Ensure HCW rename happens if original name exists
        if 'HCW.' in df.columns and 'HCW.' in rename_mapping:
             df.rename(columns={'HCW.': rename_mapping['HCW.']}, inplace=True)

        # Update numeric cols list based on renamed columns
        numeric_cols_updated = [rename_mapping.get(col, col) for col in numeric_cols_original]
        
        # Ensure Year and Week columns exist (create if missing)
        if 'Year' not in df.columns:
            print("Warning: 'Year' column not found. Checking for alternative columns...")
            # Try to find alternative column that might contain year information
            year_alternatives = ['year', 'YEAR', 'Yr', 'yr']
            found = False
            for alt in year_alternatives:
                if alt in df.columns:
                    print(f"Found alternative column '{alt}' for Year")
                    df['Year'] = df[alt]
                    found = True
                    break
            if not found:
                print("Error: Could not find Year column. Creating placeholder Year column with 2023.")
                df['Year'] = 2023  # Default year as placeholder
        
        if 'Week' not in df.columns:
            print("Warning: 'Week' column not found. Creating placeholder Week column with 1.")
            df['Week'] = 1  # Default week as placeholder

        # Process numeric columns
        for col in numeric_cols_updated:
            if col in df.columns:
                # Use a safer approach for fillna that avoids the FutureWarning
                df[col] = pd.to_numeric(df[col], errors='coerce')
                # Replace the problematic inplace=True approach
                df[col] = df[col].fillna(0)
                # Use Int64 to handle potential residual NaNs if any step failed
                try:
                    df[col] = df[col].astype(int)
                except ValueError:
                     # If conversion to int fails after filling NaNs (unlikely but possible with odd data)
                     # Keep as float or handle differently. For now, print warning.
                     print(f"Warning: Could not convert column '{col}' to int after processing. Keeping as float.")
                     df[col] = df[col].astype(float) # Keep as float if int fails
            else:
                 print(f"Warning: Expected numeric column '{col}' not found during cleaning.")


        # Standardize State names
        if 'State' in df.columns:
            df['State'] = df['State'].str.strip()
            # Add more state name corrections as needed
            df['State'] = df['State'].replace({'Fct': 'FCT', 'Nassarawa': 'Nasarawa'})
        else:
            print("Error: 'State' column not found. Creating placeholder State column.")
            # Create a placeholder State column if missing
            df['State'] = 'Unknown'

        # Ensure Year and Week are integers for sorting
        df['Year'] = pd.to_numeric(df['Year'], errors='coerce').fillna(2023).astype(int)
        df['Week'] = pd.to_numeric(df['Week'], errors='coerce').fillna(1).astype(int)
        
        # Separate state and total data
        # Add a Total row if none exists
        if 'Total' not in df['State'].values:
            print("No 'Total' row found. Creating summary row...")
            # Create a summary row for totals
            total_row = {'State': 'Total', 'Year': df['Year'].max(), 'Week': df['Week'].max()}
            # Sum numeric columns for the total row
            for col in df.columns:
                if col not in ['State', 'Year', 'Week'] and pd.api.types.is_numeric_dtype(df[col]):
                    total_row[col] = df[col].sum()
            # Add the total row to the dataframe
            df = pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)
        
        df_states = df[df['State'] != 'Total'].copy()
        df_totals = df[df['State'] == 'Total'].copy()

        # Create a combined Year-Week column for sorting totals chronologically
        # Pad week with zero if needed for correct sorting (e.g., 2021-1 -> 2021-01)
        df_totals['YearWeekSort'] = df_totals['Year'].astype(str) + '-' + df_totals['Week'].astype(str).str.zfill(2)
        df_totals.sort_values('YearWeekSort', inplace=True)

        print("Data cleaning complete.")
        return df_states, df_totals

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        return pd.DataFrame(), pd.DataFrame() # Return empty dataframes on error
    except Exception as e:
        print(f"An error occurred during data processing: {e}")
        return pd.DataFrame(), pd.DataFrame() # Return empty dataframes on error

# Load data when the script starts
df_states, df_totals = load_and_clean_data(file_path)

# Get available years and weeks for dropdowns
try:
    available_years = sorted(df_states['Year'].unique(), reverse=True)
    print(f"Available years: {available_years}")
    if not available_years:
        print("Warning: No years found in the data. Using default year.")
        available_years = [2023]
except Exception as e:
    print(f"Error getting available years: {e}")
    available_years = [2023]  # Default if there's an error
# Weeks depend on the selected year, will be updated by callback

# --- 2. Initialize Dash App ---
app = dash.Dash(__name__)
server = app.server # Expose server for potential deployment

# --- 3. Define App Layout ---
app.layout = html.Div(children=[
    html.H1(children='Nigeria Lassa Fever Dashboard', style={'textAlign': 'center'}),

    html.Div(children='Interactive dashboard showing Lassa Fever case data.', style={'textAlign': 'center', 'marginBottom': '20px'}),

    # Dropdown Filters Row
    html.Div([
        html.Div([
            html.Label('Select Year:'),
            dcc.Dropdown(
                id='year-dropdown',
                options=[{'label': year, 'value': year} for year in available_years],
                value=available_years[0] if available_years else None # Default to latest year
            )
        ], style={'width': '48%', 'display': 'inline-block', 'paddingRight': '2%'}),

        html.Div([
            html.Label('Select Week:'),
            dcc.Dropdown(
                id='week-dropdown',
                # Options will be populated by callback based on selected year
                value=None # Default to None, will be set by callback
            )
        ], style={'width': '48%', 'display': 'inline-block'})
    ], style={'marginBottom': '20px'}),

    # Graphs Row
    html.Div([
        # Bar Chart - Cases by State for selected week
        html.Div([
            dcc.Graph(id='state-bar-chart')
        ], style={'width': '48%', 'display': 'inline-block', 'verticalAlign': 'top', 'paddingRight': '2%'}),

        # Line Chart - Total Cases Trend
        html.Div([
            dcc.Graph(id='total-cases-trend')
        ], style={'width': '48%', 'display': 'inline-block', 'verticalAlign': 'top'})
    ])
])

# --- 4. Define Callbacks for Interactivity ---

# Callback to update Week dropdown based on selected Year
@app.callback(
    Output('week-dropdown', 'options'),
    Output('week-dropdown', 'value'),
    Input('year-dropdown', 'value')
)
def set_week_options(selected_year):
    if selected_year is None or df_states.empty:
        return [], None
    # Get weeks available for the selected year
    available_weeks = sorted(df_states[df_states['Year'] == selected_year]['Week'].unique())
    options = [{'label': f'Week {week}', 'value': week} for week in available_weeks]
    # Default to the latest week for that year
    value = available_weeks[-1] if available_weeks else None
    return options, value

# Callback to update graphs based on Year and Week selection
@app.callback(
    Output('state-bar-chart', 'figure'),
    Output('total-cases-trend', 'figure'),
    Input('year-dropdown', 'value'),
    Input('week-dropdown', 'value')
)
def update_graphs(selected_year, selected_week):
    if selected_year is None or selected_week is None or df_states.empty or df_totals.empty:
        # Return empty figures if no selection or data is empty
        return px.bar(title="Select Year and Week"), px.line(title="Select Year and Week")

    # Filter state data for the selected year and week
    filtered_states = df_states[(df_states['Year'] == selected_year) & (df_states['Week'] == selected_week)]
    # Sort by Confirmed cases for better visualization
    filtered_states = filtered_states.sort_values('Confirmed', ascending=False)

    # Filter total data up to the selected year and week
    # Create the sort key for comparison
    selected_year_week_sort = f"{selected_year}-{str(selected_week).zfill(2)}"
    filtered_totals = df_totals[df_totals['YearWeekSort'] <= selected_year_week_sort].copy()

    # Create Bar Chart for States
    bar_fig = px.bar(
        filtered_states,
        x='State',
        y='Confirmed',
        title=f'Confirmed Cases by State (Year: {selected_year}, Week: {selected_week})',
        labels={'Confirmed': 'Number of Confirmed Cases'},
        height=400 # Adjust height as needed
    )
    bar_fig.update_layout(xaxis_tickangle=-45) # Angle state names if many

    # Create Line Chart for Totals Trend
    line_fig = px.line(
        filtered_totals,
        x='YearWeekSort', # Use the sortable key for x-axis
        y=['Confirmed', 'Deaths'], # Plot both Confirmed and Deaths
        title=f'Total Confirmed Cases & Deaths Trend (Up to Y{selected_year}-W{selected_week})',
        labels={'YearWeekSort': 'Time (Year-Week)', 'value': 'Count', 'variable': 'Metric'}, # Nicer labels
        markers=True, # Add markers to points
        height=400 # Adjust height as needed
    )
    line_fig.update_layout(xaxis_title="Year-Week", yaxis_title="Number of Cases")


    return bar_fig, line_fig

# --- 5. Run the App ---
if __name__ == '__main__':
    # Ensure data is loaded before running the app
    if df_states.empty or df_totals.empty:
        print("Warning: One or both dataframes are empty. Creating minimal sample data for testing.")
        # Create minimal sample data if the dataframes are empty
        if df_states.empty:
            df_states = pd.DataFrame({
                'State': ['Edo', 'Ondo', 'Bauchi'],
                'Year': [2023, 2023, 2023],
                'Week': [1, 1, 1],
                'Suspected': [10, 15, 8],
                'Confirmed': [5, 7, 3],
                'Deaths': [1, 2, 1]
            })
        if df_totals.empty:
            df_totals = pd.DataFrame({
                'State': ['Total'],
                'Year': [2023],
                'Week': [1],
                'Suspected': [33],
                'Confirmed': [15],
                'Deaths': [4],
                'YearWeekSort': ['2023-01']
            })
        print("Created sample data for testing.")
    
    print("Starting Dash server...")
    app.run(debug=True) # Updated from run_server to run for newer Dash versions