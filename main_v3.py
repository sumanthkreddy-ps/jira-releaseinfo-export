import requests
import pandas as pd
from datetime import datetime
import json
from urllib3.exceptions import InsecureRequestWarning
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(InsecureRequestWarning)

class JiraReleaseFetcher:
    def __init__(self, jira_url, username, password):
        """
        Initialize Jira API client
        
        Args:
            jira_url (str): Base URL of Jira instance
            username (str): Jira username
            password (str): Jira password/API token
        """
        self.jira_url = jira_url.rstrip('/')
        self.auth = (username, password)
        self.session = requests.Session()
        self.session.verify = False
    
    def fetch_releases(self, project_keys, start_date, end_date):
        """
        Fetch releases for given projects within date range
        
        Args:
            project_keys (str): Comma-separated project keys
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            
        Returns:
            list: List of release dictionaries
        """
        releases = []
        project_list = [key.strip() for key in project_keys.split(',')]
        
        for project_key in project_list:
            print(f"Fetching releases for project: {project_key}")
            
            # Get project versions
            url = f"{self.jira_url}/rest/api/2/project/{project_key}/versions"
            
            try:
                response = self.session.get(url, auth=self.auth)
                response.raise_for_status()
                versions = response.json()
                
                for version in versions:
                    # Filter for released versions within date range
                    if (version.get('released', False) and 
                        version.get('releaseDate') and
                        self._is_date_in_range(version['releaseDate'], start_date, end_date)):
                        
                        release_info = {
                            'Project_Key': project_key,
                            'Version': version.get('name', ''),
                            'Status': 'Released' if version.get('released') else 'Unreleased',
                            'Start_Date': version.get('startDate', ''),
                            'Release_Date': version.get('releaseDate', ''),
                            'Description': version.get('description', ''),
                            'Version_ID': version.get('id', '')
                        }
                        releases.append(release_info)
                        
            except requests.exceptions.RequestException as e:
                print(f"Error fetching releases for {project_key}: {e}")
                continue
        
        return releases
    
    def _is_date_in_range(self, date_str, start_date, end_date):
        """Check if date is within the specified range"""
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
            start_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
            return start_obj <= date_obj <= end_obj
        except ValueError:
            return False
    
    def fetch_issues_for_releases(self, releases, debug_fields=False):
        """
        Fetch issues for each release version
        
        Args:
            releases (list): List of release dictionaries
            debug_fields (bool): If True, prints all available fields for debugging
            
        Returns:
            list: List of issue dictionaries
        """
        all_issues = []
        
        for release in releases:
            print(f"Fetching issues for version: {release['Version']} in project: {release['Project_Key']}")
            
            # JQL to find issues with specific fix version
            jql = f"project = '{release['Project_Key']}' AND fixVersion = '{release['Version']}'"
            
            url = f"{self.jira_url}/rest/api/2/search"
            params = {
                'jql': jql,
                'maxResults': 1000,  # Adjust as needed
                'fields': 'priority,issuetype,key,summary,assignee,reporter,status,resolution,fixVersions,labels,customfield_*,description,project',
                'expand': 'changelog'
            }
            
            try:
                response = self.session.get(url, params=params, auth=self.auth)
                response.raise_for_status()
                search_results = response.json()
                
                # Debug: Print available fields for first issue
                if debug_fields and search_results.get('issues'):
                    self._debug_custom_fields(search_results['issues'][0])
                
                for issue in search_results.get('issues', []):
                    issue_data = self._extract_issue_data(issue, release)
                    all_issues.append(issue_data)
                    
            except requests.exceptions.RequestException as e:
                print(f"Error fetching issues for version {release['Version']}: {e}")
                continue
        
        return all_issues
    
    def _extract_issue_data(self, issue, release):
        """Extract relevant data from Jira issue"""
        fields = issue.get('fields', {})
        
        # Extract basic fields
        issue_data = {
            'Project_Key': release['Project_Key'],
            'Release_Version': release['Version'],
            'Release_Date': release['Release_Date'],
            'Priority': fields.get('priority', {}).get('name', '') if fields.get('priority') else '',
            'Issue_Type': fields.get('issuetype', {}).get('name', '') if fields.get('issuetype') else '',
            'Issue_Key': issue.get('key', ''),
            'Summary': fields.get('summary', ''),
            'Assignee': fields.get('assignee', {}).get('displayName', '') if fields.get('assignee') else '',
            'Reporter': fields.get('reporter', {}).get('displayName', '') if fields.get('reporter') else '',
            'Status': fields.get('status', {}).get('name', '') if fields.get('status') else '',
            'Resolution': fields.get('resolution', {}).get('name', '') if fields.get('resolution') else '',
            'Fix_Version': ', '.join([v.get('name', '') for v in fields.get('fixVersions', [])]),
            'Labels': ', '.join(fields.get('labels', [])),
            'Description': fields.get('description', ''),
        }
        
        # Extract custom fields - Updated with your actual Jira field IDs
        custom_fields = {
            'SDLC_Information': self._get_custom_field_value(fields, ['customfield_15600']),  # SDLC Information
            'Application_Name': self._get_custom_field_value(fields, ['customfield_11700']),  # Application Name
            'Story_Points': self._get_custom_field_value(fields, ['customfield_10106']),      # Story Points
            'Sprint': self._extract_sprint_info(fields),                                       # Sprint (customfield_10104)
            'Acceptance_Criteria': self._get_custom_field_value(fields, ['customfield_10601']), # Acceptance Criteria
            'Feature_Link': self._get_custom_field_value(fields, ['customfield_10100']),      # Feature Link
            'Notes': self._get_custom_field_value(fields, ['customfield_10602'])              # Notes
        }
        
        issue_data.update(custom_fields)
        return issue_data
    
    def _get_custom_field_value(self, fields, possible_field_ids):
        """Get value from custom fields by trying multiple possible field IDs"""
        for field_id in possible_field_ids:
            if field_id in fields and fields[field_id] is not None:
                value = fields[field_id]
                if isinstance(value, dict):
                    return value.get('value', str(value))
                elif isinstance(value, list):
                    return ', '.join([str(item) for item in value])
                else:
                    return str(value)
        return ''
    
    def _debug_custom_fields(self, issue):
        """Debug function to print all available custom fields"""
        fields = issue.get('fields', {})
        print("\n" + "="*60)
        print("DEBUG: Available Custom Fields for Issue:", issue.get('key'))
        print("="*60)
        
        custom_fields = {}
        for field_name, field_value in fields.items():
            if field_name.startswith('customfield_'):
                if field_value is not None:
                    custom_fields[field_name] = field_value
        
        if not custom_fields:
            print("No custom fields with values found.")
        else:
            for field_id, value in custom_fields.items():
                print(f"{field_id}: {type(value).__name__} = {str(value)[:100]}...")
        
        print("="*60 + "\n")
        
        # Also print field names mapping if available
        print("You can also check field names at: {}/rest/api/2/field".format(self.jira_url))
        print("This will show the mapping between field IDs and field names.\n")
    
    def get_field_mappings(self):
        """
        Get all field mappings from Jira to identify custom field IDs
        
        Returns:
            dict: Mapping of field names to field IDs
        """
        url = f"{self.jira_url}/rest/api/2/field"
        
        try:
            response = self.session.get(url, auth=self.auth)
            response.raise_for_status()
            fields = response.json()
            
            field_mappings = {}
            custom_fields = {}
            
            print("\n" + "="*80)
            print("CUSTOM FIELD MAPPINGS")
            print("="*80)
            
            for field in fields:
                field_id = field.get('id', '')
                field_name = field.get('name', '')
                
                if field_id.startswith('customfield_'):
                    custom_fields[field_name] = field_id
                    print(f"{field_name:<40} -> {field_id}")
                
                field_mappings[field_name] = field_id
            
            print("="*80 + "\n")
            
            # Look for potential matches for our required fields
            required_fields = [
                'SDLC Information', 'Application Name', 'Story Points', 
                'Sprint', 'Acceptance Criteria', 'Feature Link', 'Notes'
            ]
            
            print("POTENTIAL MATCHES FOR REQUIRED FIELDS:")
            print("-" * 50)
            
            for req_field in required_fields:
                matches = []
                for field_name, field_id in custom_fields.items():
                    if any(word.lower() in field_name.lower() for word in req_field.split()):
                        matches.append((field_name, field_id))
                
                if matches:
                    print(f"\n{req_field}:")
                    for match_name, match_id in matches:
                        print(f"  - {match_name} -> {match_id}")
                else:
                    print(f"\n{req_field}: No obvious matches found")
            
            print("\n" + "-" * 50 + "\n")
            
            return field_mappings
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching field mappings: {e}")
            return {}
        """Extract sprint information from custom fields"""
        sprint_fields = ['customfield_10020', 'customfield_10007', 'customfield_10105']
        
        for field_id in sprint_fields:
            if field_id in fields and fields[field_id]:
                sprint_data = fields[field_id]
                if isinstance(sprint_data, list) and sprint_data:
                    # Get the latest sprint
                    latest_sprint = sprint_data[-1]
                    if isinstance(latest_sprint, str):
                        # Parse sprint string format
                        if 'name=' in latest_sprint:
                            start = latest_sprint.find('name=') + 5
                            end = latest_sprint.find(',', start)
                            if end == -1:
                                end = latest_sprint.find(']', start)
                            return latest_sprint[start:end] if end > start else latest_sprint
                    elif isinstance(latest_sprint, dict):
                        return latest_sprint.get('name', str(latest_sprint))
                elif isinstance(sprint_data, str):
                    return sprint_data
        return ''
    
    def create_dataframes(self, releases, issues):
        """Create pandas DataFrames from releases and issues data"""
        releases_df = pd.DataFrame(releases)
        issues_df = pd.DataFrame(issues)
        
        return releases_df, issues_df
    
    def export_to_excel(self, releases_df, issues_df, project_keys, start_date, end_date):
        """Export DataFrames to Excel file"""
        # Create filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        project_keys_clean = project_keys.replace(',', '_').replace(' ', '')
        filename = f"Jira_Releases_Issues_{project_keys_clean}_{start_date}_to_{end_date}_{timestamp}.xlsx"
        
        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                # Write releases data
                releases_df.to_excel(writer, sheet_name='Releases', index=False)
                
                # Write issues data
                issues_df.to_excel(writer, sheet_name='Issues', index=False)
                
                # Auto-adjust column widths
                for sheet_name in writer.sheets:
                    worksheet = writer.sheets[sheet_name]
                    for column in worksheet.columns:
                        max_length = 0
                        column_letter = column[0].column_letter
                        for cell in column:
                            try:
                                if len(str(cell.value)) > max_length:
                                    max_length = len(str(cell.value))
                            except:
                                pass
                        adjusted_width = min(max_length + 2, 50)  # Cap at 50 characters
                        worksheet.column_dimensions[column_letter].width = adjusted_width
            
            print(f"Data exported successfully to: {filename}")
            return filename
            
        except Exception as e:
            print(f"Error exporting to Excel: {e}")
            return None

def debug_sprint_info():
    """
    Standalone function to debug sprint information for a specific issue
    Use this to identify sprint field IDs before running the main script
    """
    # Configuration - update these with your details
    JIRA_URL = "https://your-jira-instance.com"  # Replace with your Jira URL
    USERNAME = "your-username"  # Replace with your username
    PASSWORD = "your-password"  # Replace with your password
    TEST_ISSUE_KEY = "PROJ-123"  # Replace with an actual issue key that has sprint info
    
    # Initialize Jira fetcher
    jira_fetcher = JiraReleaseFetcher(JIRA_URL, USERNAME, PASSWORD)
    
    print("🔍 DEBUGGING SPRINT INFORMATION")
    print("="*50)
    print(f"Testing with issue: {TEST_ISSUE_KEY}")
    print("Make sure this issue has sprint information!")
    print("="*50)
    
    # Debug sprint fields for the specific issue
    sprint_candidates = jira_fetcher.debug_sprint_fields_for_issue(TEST_ISSUE_KEY)
    
    if sprint_candidates:
        print("\n✅ RECOMMENDED SPRINT FIELD IDs TO USE:")
        print("-" * 40)
        for field_id, field_name, field_value in sprint_candidates:
            print(f"Field ID: {field_id}")
            print(f"Field Name: {field_name}")
            print(f"Recommended for: _extract_sprint_info() function")
            print()
    
    return sprint_candidates
    """
    Main function to execute the Jira data fetching process
    """
    # Configuration parameters
    JIRA_URL = "https://your-jira-instance.com"  # Replace with your Jira URL
    USERNAME = "your-username"  # Replace with your username
    PASSWORD = "your-password"  # Replace with your password/API token
    
    # Input parameters
    PROJECT_KEYS = "PROJ1,PROJ2,PROJ3"  # Comma-separated project keys
    START_DATE = "2024-01-01"  # Start date in YYYY-MM-DD format
    END_DATE = "2024-12-31"    # End date in YYYY-MM-DD format
    
    try:
        # Initialize Jira fetcher
        jira_fetcher = JiraReleaseFetcher(JIRA_URL, USERNAME, PASSWORD)
        
        print("Starting Jira data extraction...")
        print(f"Project Keys: {PROJECT_KEYS}")
        print(f"Date Range: {START_DATE} to {END_DATE}")
        print("-" * 50)
        
        # Fetch releases
        print("Fetching releases...")
        releases = jira_fetcher.fetch_releases(PROJECT_KEYS, START_DATE, END_DATE)
        print(f"Found {len(releases)} releases")
        
        if not releases:
            print("No releases found for the specified criteria.")
            return
        
        # Fetch issues for releases
        print("\nFetching issues for releases...")
        issues = jira_fetcher.fetch_issues_for_releases(releases, debug_fields=True)  # Enable debugging
        print(f"Found {len(issues)} issues")
        
        # Get field mappings to identify correct custom field IDs
        print("\nFetching field mappings...")
        field_mappings = jira_fetcher.get_field_mappings()
        
        # Create DataFrames
        print("\nCreating DataFrames...")
        releases_df, issues_df = jira_fetcher.create_dataframes(releases, issues)
        
        # Display summary
        print(f"\nReleases DataFrame shape: {releases_df.shape}")
        print(f"Issues DataFrame shape: {issues_df.shape}")
        
        # Export to Excel
        print("\nExporting to Excel...")
        filename = jira_fetcher.export_to_excel(releases_df, issues_df, PROJECT_KEYS, START_DATE, END_DATE)
        
        if filename:
            print(f"\n✅ Process completed successfully!")
            print(f"📊 Data exported to: {filename}")
        else:
            print("\n❌ Export failed, but DataFrames are available in memory")
            
    except Exception as e:
        print(f"❌ Error in main execution: {e}")

if __name__ == "__main__":
    main()
