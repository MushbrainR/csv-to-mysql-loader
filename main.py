from gooey import Gooey, GooeyParser
from database_operations import to_sql # Import the main function from the other file
import pandas as pd

@Gooey(
    program_name="CSV to MySQL Loader",
    progress_regex=r"^progress: (\d+)%$",
    advanced=True,
    menu=[{
        'name': 'File',
        'items': [{
            'type': 'AboutDialog',
            'menuTitle': 'About',
            'name': 'CSV to MySQL Loader',
            'description': 'A tool to load CSV data into a MySQL database',
            'version': '1.0',
            'copyright': '2023',
            'website': 'https://example.com',
            'developer': 'Your Name'
        }]
    }],
    body_bg_color='#2E3440',
    header_bg_color='#4C566A',
    header_title_color='#FFFFFF',
    header_subtitle_color='#D8DEE9',
    terminal_panel_color='#2E3440',
    terminal_font_color='#D8DEE9',
    terminal_font_size=12,
)
def main():
    parser = GooeyParser(description="Load CSV data into a MySQL database")
    parser.add_argument(
        "csv_file",
        metavar="CSV File",
        help="Select the CSV file to load",
        widget="FileChooser",
    )
    parser.add_argument(
        "host",
        metavar="Database Host",
        help="Enter the MySQL host (e.g., localhost)",
        default="localhost",
    )
    parser.add_argument(
        "user",
        metavar="Database User",
        help="Enter the MySQL username",
        default="root",
    )
    parser.add_argument(
        "password",
        metavar="Database Password",
        help="Enter the MySQL password",
        widget="PasswordField",
    )
    parser.add_argument(
        "database",
        metavar="Database Name",
        help="Enter the MySQL database name",
    )
    parser.add_argument(
        "table_name",
        metavar="Table Name",
        help="Enter the MySQL table name",
    )
    parser.add_argument(
        "column_names",
        metavar="Column Names",
        help="Enter the column names (comma-separated)",
        default="name, team, number, position, age, height, weight, college, salary",
    )
    args = parser.parse_args()

    # Load CSV file
    try:
        df = pd.read_csv(args.csv_file).dropna(how="all")
    except Exception as e:
        print(f"Error loading CSV file: {e}")
        return

    # Database configuration
    db_config = {
        "host": args.host,
        "user": args.user,
        "password": args.password,
        "database": args.database,
    }

    # Call the main function from the other file
    to_sql(df, args.table_name, args.column_names, db_config)

if __name__ == "__main__":
    main()